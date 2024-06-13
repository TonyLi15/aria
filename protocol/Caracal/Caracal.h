//
// Created by Haowen Li on 6/5/24
//

#pragma once

#include "core/Table.h"
#include "protocol/Caracal/CaracalHelper.h"
#include "protocol/Caracal/CaracalMessage.h"
#include "protocol/Caracal/CaracalPartitioner.h"
#include "protocol/Caracal/CaracalTransaction.h"

namespace aria {

template <class Database> class Caracal {
public:
	using DatabaseType = Database;
  	using MetaDataType = std::atomic<uint64_t>;
  	using ContextType = typename DatabaseType::ContextType;
  	using MessageType = CaracalMessage;
  	using TransactionType = CaracalTransaction;

	using MessageFactoryType = CaracalMessageFactory;
	using MessageHandlerType = CaracalHandler;

	Caracal(DatabaseType &db, CaracalPartitioner &partitioner) 
		: db(db), partitioner(partitioner) {}

	void abort(TransactionType &txn,
				std::vector<std::unique_ptr<Message>> &messages) {
		
	}

	void commit(TransactionType &txn,
				std::vector<std::unique_ptr<Message>> &messages) {
		
	}

private:
	DatabaseType &db;
	CaracalPartitioner &partitioner;
};
} // namespace aria