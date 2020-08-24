if (NANO_ROCKSDB)
	set (rocksdb_libs ${ROCKSDB_LIBRARIES} ${ZLIB_LIBRARIES})
	set (rocksdb_sources rocksdb/rocksdb.hpp rocksdb/rocksdb.cpp rocksdb/rocksdb_iterator.hpp rocksdb/rocksdb_txn.hpp rocksdb/rocksdb_txn.cpp)
endif ()

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
	# No opencl
elseif (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
	set (platform_sources plat/windows/openclapi.cpp)
	set (psapi_lib Psapi)
	if (NANO_ROCKSDB)
		set (rocksdb_libs ${rocksdb_libs} Shlwapi Rpcrt4)
	endif ()
elseif (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
	set (platform_sources plat/posix/openclapi.cpp)
elseif (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
	set (platform_sources plat/posix/openclapi.cpp)
else ()
	error ("Unknown platform: ${CMAKE_SYSTEM_NAME}")
endif ()

add_library (node
	${platform_sources}
	${rocksdb_sources}
	active_transactions.hpp
	active_transactions.cpp
	blockprocessor.hpp
	blockprocessor.cpp
	bootstrap/bootstrap_attempt.hpp
	bootstrap/bootstrap_attempt.cpp
	bootstrap/bootstrap_bulk_pull.hpp
	bootstrap/bootstrap_bulk_pull.cpp
	bootstrap/bootstrap_bulk_push.hpp
	bootstrap/bootstrap_bulk_push.cpp
	bootstrap/bootstrap_connections.hpp
	bootstrap/bootstrap_connections.cpp
	bootstrap/bootstrap_frontier.hpp
	bootstrap/bootstrap_frontier.cpp
	bootstrap/bootstrap_lazy.hpp
	bootstrap/bootstrap_lazy.cpp
	bootstrap/bootstrap_server.hpp
	bootstrap/bootstrap_server.cpp
	bootstrap/bootstrap.hpp
	bootstrap/bootstrap.cpp
	cli.hpp
	cli.cpp
	common.hpp
	common.cpp
	confirmation_height_bounded.hpp
	confirmation_height_bounded.cpp
	confirmation_height_processor.hpp
	confirmation_height_processor.cpp
	confirmation_height_unbounded.hpp
	confirmation_height_unbounded.cpp
	confirmation_solicitor.hpp
	confirmation_solicitor.cpp
	daemonconfig.hpp
	daemonconfig.cpp
	distributed_work.hpp
	distributed_work.cpp
	distributed_work_factory.hpp
	distributed_work_factory.cpp
	election.hpp
	election.cpp
	environment.hpp
	environment.cpp
	gap_cache.hpp
	gap_cache.cpp
	ipc/action_handler.hpp
	ipc/action_handler.cpp
	ipc/flatbuffers_handler.hpp
	ipc/flatbuffers_handler.cpp
	ipc/flatbuffers_util.hpp
	ipc/flatbuffers_util.cpp
	ipc/ipc_access_config.hpp
	ipc/ipc_access_config.cpp
	ipc/ipc_broker.hpp
	ipc/ipc_broker.cpp
	ipc/ipc_config.hpp
	ipc/ipc_config.cpp
	ipc/ipc_server.hpp
	ipc/ipc_server.cpp
	json_handler.hpp
	json_handler.cpp
	json_payment_observer.hpp	
	json_payment_observer.cpp
	lmdb/lmdb.hpp
	lmdb/lmdb.cpp
	lmdb/lmdb_env.hpp
	lmdb/lmdb_env.cpp
	lmdb/lmdb_iterator.hpp
	lmdb/lmdb_txn.hpp
	lmdb/lmdb_txn.cpp
	lmdb/wallet_value.hpp
	lmdb/wallet_value.cpp
	logging.hpp
	logging.cpp
	network.hpp
	network.cpp
	nodeconfig.hpp
	nodeconfig.cpp
	node_observers.hpp
	node_observers.cpp
	node_rpc_config.hpp
	node_rpc_config.cpp
	node.hpp
	node.cpp
	online_reps.hpp
	online_reps.cpp
	openclconfig.hpp
	openclconfig.cpp
	openclwork.hpp
	openclwork.cpp
	payment_observer_processor.hpp
	payment_observer_processor.cpp
	peer_exclusion.hpp
	peer_exclusion.cpp
	portmapping.hpp
	portmapping.cpp
	node_pow_server_config.hpp
	node_pow_server_config.cpp
	repcrawler.hpp
	repcrawler.cpp
	request_aggregator.hpp
	request_aggregator.cpp
	signatures.hpp
	signatures.cpp
	socket.hpp
	socket.cpp
	state_block_signature_verification.hpp
	state_block_signature_verification.cpp
	telemetry.hpp
	telemetry.cpp
	testing.hpp
	testing.cpp
	transport/tcp.hpp
	transport/tcp.cpp
	transport/transport.hpp
	transport/transport.cpp
	transport/udp.hpp
	transport/udp.cpp
	vote_processor.hpp
	vote_processor.cpp
	voting.hpp
	voting.cpp
	wallet.hpp
	wallet.cpp
	websocket.hpp
	websocket.cpp
	websocketconfig.hpp
	websocketconfig.cpp
	write_database_queue.hpp
	write_database_queue.cpp
	xorshift.hpp)

target_link_libraries (node
	rpc
	secure
	nano_lib
	libminiupnpc-static
	argon2
	lmdb
	Boost::filesystem
	Boost::log_setup
	Boost::log
	Boost::program_options
	Boost::system
	Boost::thread
	Boost::boost
	${rocksdb_libs}
	${CMAKE_DL_LIBS}
	${psapi_lib}
	)

if (NANO_STACKTRACE_BACKTRACE)
	target_link_libraries(node backtrace)
endif ()

target_compile_definitions(node
	PRIVATE
		-DTAG_VERSION_STRING=${TAG_VERSION_STRING}
		-DGIT_COMMIT_HASH=${GIT_COMMIT_HASH})

# This ensures that any changes to Flatbuffers source files will cause a
# regeneration of any C++ header files.
add_dependencies(node ipc_flatbuffers_lib)
