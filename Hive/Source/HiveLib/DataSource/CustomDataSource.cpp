#include "CustomDataSource.h"
#include "Database/Database.h"

#include <boost/algorithm/string/replace.hpp>
#include <boost/lexical_cast.hpp>

using boost::lexical_cast;
using boost::bad_lexical_cast;

CustomDataSource::CustomDataSource(Poco::Logger& logger, shared_ptr<Database> db) : SqlDataSource(logger, db) { }
CustomDataSource::~CustomDataSource() {}

void CustomDataSource::populateQuery(string query, Sqf::Parameters& params, CustomDataQueue& queue)
{

	for (int i = 0; i < params.size(); i++)
	{
		query = boost::algorithm::replace_nth_copy(query, "?", i, Sqf::GetStringAny(params.at(i)));
	}

	auto custRes = getDB()->queryParams(query.c_str());

	while (custRes->fetchRow())
	{
		Sqf::Parameters custParams;

		for (int i = 0; i < custRes->numFields(); i++)
		{
			int val = custRes->at(i).getInt32();

			if (val == 0 && custRes->at(i).getString() != "0")
			{
				custParams.push_back(custRes->at(i).getString());
			}
			else
			{
				custParams.push_back(val);
			}
		}

		queue.push(custParams);
	}
}
bool CustomDataSource::customExecute(string query, Sqf::Parameters& params) {
	static SqlStatementID stmtId;
	
	unique_ptr<SqlStatement> stmt;

	stmt = getDB()->makeStatement(stmtId, query);

	for (int i = 0; i < params.size(); i++)
	{
		try
		{
			stmt->addInt32(lexical_cast<int>(params.at(i)));
		}
		catch (bad_lexical_cast)
		{
			stmt->addString(lexical_cast<string>(params.at(i)));
		}
	}

	bool exRes = stmt->execute();
	poco_assert(exRes == true);

	return exRes;
}