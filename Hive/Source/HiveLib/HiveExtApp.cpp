/*
* Copyright (C) 2009-2012 Rajko Stojadinovic <http://github.com/rajkosto/hive>
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program; if not, write to the Free Software
* Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
*/

#include "HiveExtApp.h"

#include <boost/bind.hpp>
#include <boost/optional.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <boost/date_time/gregorian_calendar.hpp>

void HiveExtApp::setupClock()
{
	namespace pt = boost::posix_time;
	pt::ptime utc = pt::second_clock::universal_time();
	pt::ptime now;

	Poco::AutoPtr<Poco::Util::AbstractConfiguration> timeConf(config().createView("Time"));
	string timeType = timeConf->getString("Type","Local");

	if (boost::iequals(timeType,"Custom"))
	{
		now = utc;

		const char* defOffset = "0";
		string offsetStr = timeConf->getString("Offset",defOffset);
		boost::trim(offsetStr);
		if (offsetStr.length() < 1)
			offsetStr = defOffset;
		
		try
		{
			now += pt::duration_from_string(offsetStr);
		}
		catch(const std::exception&)
		{
			logger().warning("Invalid value for Time.Offset configuration variable (expected int, given: "+offsetStr+")");
		}
	}
	else if (boost::iequals(timeType,"Static"))
	{
		now = pt::second_clock::local_time();
		try
		{
			int hourOfTheDay = timeConf->getInt("Hour");
			now -= pt::time_duration(now.time_of_day().hours(),0,0);
			now += pt::time_duration(hourOfTheDay,0,0);
		}
		//do not change hour of the day if bad or missing value in config
		catch(const Poco::NotFoundException&) {}
		catch(const Poco::SyntaxException&) 
		{
			string hourStr = timeConf->getString("Hour","");
			boost::trim(hourStr);
			if (hourStr.length() > 0)
				logger().warning("Invalid value for Time.Hour configuration variable (expected int, given: "+hourStr+")");
		}

		//change the date
		{
			string dateStr = timeConf->getString("Date","");
			boost::trim(dateStr);
			if (dateStr.length() > 0) //only if non-empty value
			{
				namespace gr = boost::gregorian;
				try
				{
					gr::date newDate = gr::from_uk_string(dateStr);
					now = pt::ptime(newDate, now.time_of_day());
				}
				catch(const std::exception&)
				{
					logger().warning("Invalid value for Time.Date configuration variable (expected date, given: "+dateStr+")");
				}
			}
		}
	}
	else
		now = pt::second_clock::local_time();

	_timeOffset = now - utc;
}

#include "Version.h"

int HiveExtApp::main( const std::vector<std::string>& args )
{
	logger().information("HiveExt Axe Cop Edition v2");
	setupClock();

	if (!this->initialiseService())
	{
		logger().close();
		return EXIT_IOERR;
	}

	return EXIT_OK;
}

HiveExtApp::HiveExtApp(string suffixDir) : AppServer("HiveExt",suffixDir), _serverId(-1)
{
	//server and object stuff
	handlers[302] = boost::bind(&HiveExtApp::streamObjects,this,_1);		//Returns object count, superKey first time, rows after that
	handlers[303] = boost::bind(&HiveExtApp::objectInventory,this,_1,false);
	handlers[304] = boost::bind(&HiveExtApp::objectDelete,this,_1,false);
	handlers[305] = boost::bind(&HiveExtApp::vehicleMoved,this,_1);
	handlers[306] = boost::bind(&HiveExtApp::vehicleDamaged,this,_1);
	handlers[307] = boost::bind(&HiveExtApp::getDateTime,this,_1);
	handlers[308] = boost::bind(&HiveExtApp::objectPublish,this,_1);

	// Custom to just return db ID for object UID
	handlers[388] = boost::bind(&HiveExtApp::objectReturnId,this,_1);
	// for maintain 
	handlers[396] = boost::bind(&HiveExtApp::datestampObjectUpdate,this,_1,false);
	handlers[397] = boost::bind(&HiveExtApp::datestampObjectUpdate,this,_1,true);
	// For traders 
	handlers[398] = boost::bind(&HiveExtApp::tradeObject,this,_1);
	handlers[399] = boost::bind(&HiveExtApp::loadTraderDetails,this,_1);
	// End custom

	handlers[309] = boost::bind(&HiveExtApp::objectInventory,this,_1,true);
	handlers[310] = boost::bind(&HiveExtApp::objectDelete,this,_1,true);
	handlers[400] = boost::bind(&HiveExtApp::serverShutdown,this,_1);		//Shut down the hiveExt instance
	//player/character loads
	handlers[100] = boost::bind(&HiveExtApp::loadCharacters, this, _1);
	handlers[101] = boost::bind(&HiveExtApp::loadPlayer,this,_1);
	handlers[102] = boost::bind(&HiveExtApp::loadCharacterDetails,this,_1);
	handlers[103] = boost::bind(&HiveExtApp::recordCharacterLogin,this,_1);
	//character updates
	handlers[201] = boost::bind(&HiveExtApp::playerUpdate,this,_1);
	handlers[202] = boost::bind(&HiveExtApp::playerDeath,this,_1);
	handlers[203] = boost::bind(&HiveExtApp::playerInit,this,_1);

	//custom procedures
	handlers[998] = boost::bind(&HiveExtApp::customExecute, this, _1);
	handlers[999] = boost::bind(&HiveExtApp::streamCustom, this, _1);
}

#include <boost/lexical_cast.hpp>
using boost::lexical_cast;
using boost::bad_lexical_cast;

void HiveExtApp::callExtension( const char* function, char* output, size_t outputSize )
{
	Sqf::Parameters params;
	try
	{
		params = lexical_cast<Sqf::Parameters>(function);	
	}
	catch(bad_lexical_cast)
	{
		logger().error("Cannot parse function: " + string(function));
		return;
	}

	int funcNum = -1;
	try
	{
		string childIdent = boost::get<string>(params.at(0));
		if (childIdent != "CHILD")
			throw std::runtime_error("First element in parameters must be CHILD");

		params.erase(params.begin());
		funcNum = boost::get<int>(params.at(0));
		params.erase(params.begin());
	}
	catch (...)
	{
		logger().error("Invalid function format: " + string(function));
		return;
	}

	if (handlers.count(funcNum) < 1)
	{
		logger().error("Invalid method id: " + lexical_cast<string>(funcNum));
		return;
	}

	if (logger().debug())
		logger().debug("Original params: |" + string(function) + "|");

	logger().information("Method: " + lexical_cast<string>(funcNum) + " Params: " + lexical_cast<string>(params));
	HandlerFunc handler = handlers[funcNum];
	Sqf::Value res;
	boost::optional<ServerShutdownException> shutdownExc;
	try
	{
		res = handler(params);
	}
	catch (const ServerShutdownException& e)
	{
		if (!e.keyMatches(_initKey))
		{
			logger().error("Actually not shutting down");
			return;
		}

		shutdownExc = e;
		res = e.getReturnValue();
	}
	catch (...)
	{
		logger().error("Error executing |" + string(function) + "|");
		return;
	}		

	string serializedRes = lexical_cast<string>(res);
	logger().information("Result: " + serializedRes);

	if (serializedRes.length() >= outputSize)
		logger().error("Output size too big ("+lexical_cast<string>(serializedRes.length())+") for request : " + string(function));
	else
		strncpy_s(output,outputSize,serializedRes.c_str(),outputSize-1);

	if (shutdownExc.is_initialized())
		throw *shutdownExc;
}

namespace
{
	Sqf::Parameters ReturnStatus(std::string status, Sqf::Parameters rest)
	{
		Sqf::Parameters outRet;
		outRet.push_back(std::move(status));
		for (size_t i=0; i<rest.size(); i++)
			outRet.push_back(std::move(rest[i]));

		return outRet;
	}
	template<typename T>
	Sqf::Parameters ReturnStatus(std::string status, T other)
	{
		Sqf::Parameters rest; rest.push_back(std::move(other));
		return ReturnStatus(std::move(status),std::move(rest));
	}
	Sqf::Parameters ReturnStatus(std::string status)
	{
		return ReturnStatus(std::move(status),Sqf::Parameters());
	}

	Sqf::Parameters ReturnBooleanStatus(bool isGood, string errorMsg = "")
	{
		string retStatus = "PASS";
		if (!isGood)
			retStatus = "ERROR";

		if (errorMsg.length() < 1)
			return ReturnStatus(std::move(retStatus));
		else
			return ReturnStatus(std::move(retStatus),std::move(errorMsg));
	}
};

Sqf::Value HiveExtApp::getDateTime( Sqf::Parameters params )
{
	namespace pt=boost::posix_time;
	pt::ptime now = pt::second_clock::universal_time() + _timeOffset;

	Sqf::Parameters retVal;
	retVal.push_back(string("PASS"));
	{
		Sqf::Parameters dateTime;
		dateTime.push_back(static_cast<int>(now.date().year()));
		dateTime.push_back(static_cast<int>(now.date().month()));
		dateTime.push_back(static_cast<int>(now.date().day()));
		dateTime.push_back(static_cast<int>(now.time_of_day().hours()));
		dateTime.push_back(static_cast<int>(now.time_of_day().minutes()));
		retVal.push_back(dateTime);
	}
	return retVal;
}

#include <Poco/HexBinaryEncoder.h>
#include <Poco/HexBinaryDecoder.h>

#include "DataSource/ObjDataSource.h"
#include <Poco/RandomStream.h>

Sqf::Value HiveExtApp::streamObjects( Sqf::Parameters params )
{
	if (_srvObjects.empty())
	{
		if (_initKey.length() < 1)
		{
			int serverId = boost::get<int>(params.at(0));
			setServerId(serverId);

			_objData->populateObjects(getServerId(), _srvObjects);
			//set up initKey
			{
				boost::array<UInt8,16> keyData;
				Poco::RandomInputStream().read((char*)keyData.c_array(),keyData.size());
				std::ostringstream ostr;
				Poco::HexBinaryEncoder enc(ostr);
				enc.rdbuf()->setLineLength(0);
				enc.write((const char*)keyData.data(),keyData.size());
				enc.close();
				_initKey = ostr.str();
			}

			Sqf::Parameters retVal;
			retVal.push_back(string("ObjectStreamStart"));
			retVal.push_back(static_cast<int>(_srvObjects.size()));
			retVal.push_back(_initKey);
			return retVal;
		}
		else
		{
			Sqf::Parameters retVal;
			retVal.push_back(string("ERROR"));
			retVal.push_back(string("Instance already initialized"));
			return retVal;
		}
	}
	else
	{
		Sqf::Parameters retVal = _srvObjects.front();
		_srvObjects.pop();

		return retVal;
	}
}

Sqf::Value HiveExtApp::objectInventory( Sqf::Parameters params, bool byUID /*= false*/ )
{
	Int64 objectIdent = Sqf::GetBigInt(params.at(0));
	Sqf::Value inventory = boost::get<Sqf::Parameters>(params.at(1));

	if (objectIdent != 0) //all the vehicles have objectUID = 0, so it would be bad to update those
		return ReturnBooleanStatus(_objData->updateObjectInventory(getServerId(),objectIdent,byUID,inventory));

	return ReturnBooleanStatus(true);
}

Sqf::Value HiveExtApp::objectDelete( Sqf::Parameters params, bool byUID /*= false*/ )
{
	Int64 objectIdent = Sqf::GetBigInt(params.at(0));

	if (objectIdent != 0) //all the vehicles have objectUID = 0, so it would be bad to delete those
		return ReturnBooleanStatus(_objData->deleteObject(getServerId(),objectIdent,byUID));

	return ReturnBooleanStatus(true);
}

Sqf::Value HiveExtApp::datestampObjectUpdate(Sqf::Parameters params, bool byUID /*= false*/)
{
	Int64 objectIdent = Sqf::GetBigInt(params.at(0));

	if (objectIdent != 0) //all the vehicles have objectUID = 0, so it would be bad to delete those
		return ReturnBooleanStatus(_objData->updateDatestampObject(getServerId(), objectIdent, byUID));

	return ReturnBooleanStatus(true);
}

Sqf::Value HiveExtApp::vehicleMoved( Sqf::Parameters params )
{
	Int64 objectIdent = Sqf::GetBigInt(params.at(0));
	Sqf::Value worldspace = boost::get<Sqf::Parameters>(params.at(1));
	double fuel = Sqf::GetDouble(params.at(2));

	if (objectIdent > 0) //sometimes script sends this with object id 0, which is bad
		return ReturnBooleanStatus(_objData->updateVehicleMovement(getServerId(),objectIdent,worldspace,fuel));

	return ReturnBooleanStatus(true);
}

Sqf::Value HiveExtApp::vehicleDamaged( Sqf::Parameters params )
{
	Int64 objectIdent = Sqf::GetBigInt(params.at(0));
	Sqf::Value hitPoints = boost::get<Sqf::Parameters>(params.at(1));
	double damage = Sqf::GetDouble(params.at(2));

	if (objectIdent > 0) //sometimes script sends this with object id 0, which is bad
		return ReturnBooleanStatus(_objData->updateVehicleStatus(getServerId(),objectIdent,hitPoints,damage));

	return ReturnBooleanStatus(true);
}

Sqf::Value HiveExtApp::objectPublish( Sqf::Parameters params )
{
	string className = boost::get<string>(params.at(1));
	double damage = Sqf::GetDouble(params.at(2));
	int characterId = Sqf::GetIntAny(params.at(3));
	Sqf::Value worldSpace = boost::get<Sqf::Parameters>(params.at(4));
	Sqf::Value inventory = boost::get<Sqf::Parameters>(params.at(5));
	Sqf::Value hitPoints = boost::get<Sqf::Parameters>(params.at(6));
	double fuel = Sqf::GetDouble(params.at(7));
	Int64 uniqueId = Sqf::GetBigInt(params.at(8));

	return ReturnBooleanStatus(_objData->createObject(getServerId(),className,damage,characterId,worldSpace,inventory,hitPoints,fuel,uniqueId));
}

Sqf::Value HiveExtApp::objectReturnId( Sqf::Parameters params )
{
	Int64 ObjectUID = Sqf::GetBigInt(params.at(0));
	return _objData->fetchObjectId(getServerId(),ObjectUID);
}

#include "DataSource/CharDataSource.h"

Sqf::Value HiveExtApp::loadCharacters( Sqf::Parameters params )
{
	string playerId = Sqf::GetStringAny(params.at(0));

	return _charData->fetchCharacters(playerId);
}

Sqf::Value HiveExtApp::loadPlayer( Sqf::Parameters params )
{
	string playerId = Sqf::GetStringAny(params.at(0));
	string playerName = Sqf::GetStringAny(params.at(2));
	int characterSlot = Sqf::GetIntAny(params.at(3));

	return _charData->fetchCharacterInitial(playerId, getServerId(), playerName, characterSlot);
}

Sqf::Value HiveExtApp::loadCharacterDetails( Sqf::Parameters params )
{
	int characterId = Sqf::GetIntAny(params.at(0));
	
	return _charData->fetchCharacterDetails(characterId);
}

Sqf::Value HiveExtApp::loadTraderDetails( Sqf::Parameters params )
{
	if (_srvObjects.empty())
	{
		int characterId = Sqf::GetIntAny(params.at(0));

		_objData->populateTraderObjects(characterId, _srvObjects);

		Sqf::Parameters retVal;
		retVal.push_back(string("ObjectStreamStart"));
		retVal.push_back(static_cast<int>(_srvObjects.size()));
		return retVal;
	}
	else
	{
		Sqf::Parameters retVal = _srvObjects.front();
		_srvObjects.pop();

		return retVal;
	}
}

Sqf::Value HiveExtApp::tradeObject( Sqf::Parameters params )
{
	int traderObjectId = Sqf::GetIntAny(params.at(0));
	int action = Sqf::GetIntAny(params.at(1));
	return _charData->fetchTraderObject(traderObjectId, action);
}

Sqf::Value HiveExtApp::recordCharacterLogin( Sqf::Parameters params )
{
	string playerId = Sqf::GetStringAny(params.at(0));
	int characterId = Sqf::GetIntAny(params.at(1));
	int action = Sqf::GetIntAny(params.at(2));

	return ReturnBooleanStatus(_charData->recordLogin(playerId,characterId,action));
}

Sqf::Value HiveExtApp::playerUpdate( Sqf::Parameters params )
{
	int characterId = Sqf::GetIntAny(params.at(0));
	CharDataSource::FieldsType fields;

	try
	{
		if (!Sqf::IsNull(params.at(1)))
		{
			Sqf::Parameters worldSpaceArr = boost::get<Sqf::Parameters>(params.at(1));
			if (worldSpaceArr.size() > 0)
			{
				Sqf::Value worldSpace = worldSpaceArr;
				fields["Worldspace"] = worldSpace;
			}
		}
		if (!Sqf::IsNull(params.at(2)))
		{
			Sqf::Parameters inventoryArr = boost::get<Sqf::Parameters>(params.at(2));
			if (inventoryArr.size() > 0)
			{
				Sqf::Value inventory = inventoryArr;
				fields["Inventory"] = inventory;
			}
		}
		if (!Sqf::IsNull(params.at(3)))
		{
			Sqf::Parameters backpackArr = boost::get<Sqf::Parameters>(params.at(3));
			if (backpackArr.size() > 0)
			{
				Sqf::Value backpack = backpackArr;
				fields["Backpack"] = backpack;
			}
		}
		if (!Sqf::IsNull(params.at(4)))
		{
			Sqf::Parameters medicalArr = boost::get<Sqf::Parameters>(params.at(4));
			if (medicalArr.size() > 0)
			{
				for (size_t i=0;i<medicalArr.size();i++)
				{
					if (Sqf::IsAny(medicalArr[i]))
					{
						logger().warning("update.medical["+lexical_cast<string>(i)+"] changed from any to []");
						medicalArr[i] = Sqf::Parameters();
					}
				}
				Sqf::Value medical = medicalArr;
				fields["Medical"] = medical;
			}
		}
		if (!Sqf::IsNull(params.at(5)))
		{
			bool justAte = boost::get<bool>(params.at(5));
			if (justAte) fields["JustAte"] = true;
		}
		if (!Sqf::IsNull(params.at(6)))
		{
			bool justDrank = boost::get<bool>(params.at(6));
			if (justDrank) fields["JustDrank"] = true;
		}
		if (!Sqf::IsNull(params.at(7)))
		{
			int moreKillsZ = boost::get<int>(params.at(7));
			if (moreKillsZ > 0) fields["KillsZ"] = moreKillsZ;
		}
		if (!Sqf::IsNull(params.at(8)))
		{
			int moreKillsH = boost::get<int>(params.at(8));
			if (moreKillsH > 0) fields["HeadshotsZ"] = moreKillsH;
		}
		if (!Sqf::IsNull(params.at(9)))
		{
			int distanceWalked = static_cast<int>(Sqf::GetDouble(params.at(9)));
			if (distanceWalked > 0) fields["DistanceFoot"] = distanceWalked;
		}
		if (!Sqf::IsNull(params.at(10)))
		{
			int durationLived = static_cast<int>(Sqf::GetDouble(params.at(10)));
			if (durationLived > 0) fields["Duration"] = durationLived;
		}
		if (!Sqf::IsNull(params.at(11)))
		{
			Sqf::Parameters currentStateArr = boost::get<Sqf::Parameters>(params.at(11));
			if (currentStateArr.size() > 0)
			{
				Sqf::Value currentState = currentStateArr;
				fields["CurrentState"] = currentState;
			}
		}
		if (!Sqf::IsNull(params.at(12)))
		{
			int moreKillsHuman = boost::get<int>(params.at(12));
			if (moreKillsHuman > 0) fields["KillsH"] = moreKillsHuman;
		}
		if (!Sqf::IsNull(params.at(13)))
		{
			int moreKillsBandit = boost::get<int>(params.at(13));
			if (moreKillsBandit > 0) fields["KillsB"] = moreKillsBandit;
		}
		if (!Sqf::IsNull(params.at(14)))
		{
			string newModel = boost::get<string>(params.at(14));
			fields["Model"] = newModel;
		}
		if (!Sqf::IsNull(params.at(15)))
		{
			int humanityDiff = static_cast<int>(Sqf::GetDouble(params.at(15)));
			if (humanityDiff != 0) fields["Humanity"] = humanityDiff;
		}
		if (!Sqf::IsNull(params.at(16)))
		{
			int Money = static_cast<int>(Sqf::GetDouble(params.at(16)));
			if (Money != 0) fields["Money"] = Money;
		}
	}
	catch (const std::out_of_range&)
	{
		logger().warning("Update of character " + lexical_cast<string>(characterId) + " only had " + lexical_cast<string>(params.size()) + " parameters out of 17");
	}

	if (fields.size() > 0)
		return ReturnBooleanStatus(_charData->updateCharacter(characterId,getServerId(),fields));

	return ReturnBooleanStatus(true);
}

Sqf::Value HiveExtApp::playerInit( Sqf::Parameters params )
{
	int characterId = Sqf::GetIntAny(params.at(0));
	Sqf::Value inventory = boost::get<Sqf::Parameters>(params.at(1));
	Sqf::Value backpack = boost::get<Sqf::Parameters>(params.at(2));

	return ReturnBooleanStatus(_charData->initCharacter(characterId,inventory,backpack));
}

Sqf::Value HiveExtApp::playerDeath( Sqf::Parameters params )
{
	int characterId = Sqf::GetIntAny(params.at(0));
	int duration = static_cast<int>(Sqf::GetDouble(params.at(1)));
	int infected = Sqf::GetIntAny(params.at(2));
	
	return ReturnBooleanStatus(_charData->killCharacter(characterId,duration,infected));
}

Sqf::Value HiveExtApp::streamCustom(Sqf::Parameters params)
{
	if (_custQueue.empty())
	{
		string query = Sqf::GetStringAny(params.at(0));
		Sqf::Parameters rawParams = boost::get<Sqf::Parameters>(params.at(1));
		_customData->populateQuery(query, rawParams, _custQueue);
		Sqf::Parameters retVal;
		retVal.push_back(string("CustomStreamStart"));
		retVal.push_back(static_cast<int>(_custQueue.size()));

		return retVal;
	}
	else
	{
		Sqf::Parameters retVal = _custQueue.front();
		_custQueue.pop();

		return retVal;
	}
}

Sqf::Value HiveExtApp::customExecute(Sqf::Parameters params)
{
	string query = Sqf::GetStringAny(params.at(0));
	Sqf::Parameters rawParams = boost::get<Sqf::Parameters>(params.at(1));

	return _customData->customExecute(query, rawParams);
}

Sqf::Value HiveExtApp::serverShutdown( Sqf::Parameters params )
{
	string theirKey = boost::get<string>(params.at(0));
	if ((_initKey.length() > 0) && (theirKey == _initKey))
	{
		logger().information("Shutting down HiveExt instance");
		throw ServerShutdownException(theirKey,ReturnBooleanStatus(true));
	}

	return ReturnBooleanStatus(false);
}