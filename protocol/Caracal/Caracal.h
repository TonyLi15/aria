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
	using MessageHandlerType = CaracalMessageHandler;

	Caracal(DatabaseType &db, CaracalPartitioner &partitioner) 
		: db(db), partitioner(partitioner) {}

	void abort(TransactionType &txn,
				std::vector<std::unique_ptr<Message>> &messages) {
		txn.load_read_count();
		txn.clear_execution_bit();
		txn.abort_read_not_ready = false;
	}

	void commit(TransactionType &txn,
				std::vector<std::unique_ptr<Message>> &messages) {
		auto &writeSet = txn.writeSet;
		for (auto i = 0u; i < writeSet.size(); i++) {
		auto &writeKey = writeSet[i];
		auto tableId = writeKey.get_table_id();
		auto partitionId = writeKey.get_partition_id();
		auto table = db.find_table(tableId, partitionId);
		auto key = writeKey.get_key();
		auto value = writeKey.get_value();
		if (partitioner.has_master_partition(partitionId)) {
			std::atomic<uint64_t> &placeholder =
				table->search_metadata(key, txn.id);
			CHECK(BohmHelper::is_placeholder_ready(placeholder) == false);
			table->update(key, value, txn.id);
			BohmHelper::set_placeholder_to_ready(placeholder);
		} else {
			auto coordinatorID = partitioner.master_coordinator(partitionId);
			txn.network_size += MessageFactoryType::new_write_message(
				*messages[coordinatorID], *table, txn.id, key, value);
		}
		}
	}

private:
	DatabaseType &db;
	CaracalPartitioner &partitioner;
};
} // namespace aria