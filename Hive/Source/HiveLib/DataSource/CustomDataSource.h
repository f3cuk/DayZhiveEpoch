#pragma once

#include "SqlDataSource.h"
#include "DataSource.h"

class CustomDataSource : public SqlDataSource
{
public:
	typedef std::queue<Sqf::Parameters> CustomDataQueue;

	CustomDataSource(Poco::Logger& logger, shared_ptr<Database> db);
	~CustomDataSource();

	bool customExecute( string query, Sqf::Parameters& params );
	void populateQuery( string query, Sqf::Parameters& params, CustomDataQueue& queue );
};