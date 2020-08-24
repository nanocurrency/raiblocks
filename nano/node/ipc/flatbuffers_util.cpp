#include <nano/lib/blocks.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/node/ipc/flatbuffers_util.hpp>
#include <nano/secure/common.hpp>

std::unique_ptr<nanoapi::BlockStateT> nano::ipc::flatbuffers_builder::from (nano::state_block const & block_a, nano::amount const & amount_a, bool is_state_send_a)
{
	static nano::environment_constants constants;
	auto block (std::make_unique<nanoapi::BlockStateT> ());
	block->account = block_a.account ().to_account ();
	block->hash = block_a.hash ().to_string ();
	block->previous = block_a.previous ().to_string ();
	block->representative = block_a.representative ().to_account ();
	block->balance = block_a.balance ().to_string_dec ();
	block->link = block_a.link ().to_string ();
	block->link_as_account = block_a.link ().to_account ();
	block_a.signature.encode_hex (block->signature);
	block->work = nano::to_string_hex (block_a.work);

	if (is_state_send_a)
	{
		block->subtype = nanoapi::BlockSubType::BlockSubType_send;
	}
	else if (block_a.link ().is_zero ())
	{
		block->subtype = nanoapi::BlockSubType::BlockSubType_change;
	}
	else if (amount_a == 0 && constants.ledger.epochs.is_epoch_link (block_a.link ()))
	{
		block->subtype = nanoapi::BlockSubType::BlockSubType_epoch;
	}
	else
	{
		block->subtype = nanoapi::BlockSubType::BlockSubType_receive;
	}
	return block;
}

std::unique_ptr<nanoapi::BlockSendT> nano::ipc::flatbuffers_builder::from (nano::send_block const & block_a)
{
	auto block (std::make_unique<nanoapi::BlockSendT> ());
	block->hash = block_a.hash ().to_string ();
	block->balance = block_a.balance ().to_string_dec ();
	block->destination = block_a.hashables.destination.to_account ();
	block->previous = block_a.previous ().to_string ();
	block_a.signature.encode_hex (block->signature);
	block->work = nano::to_string_hex (block_a.work);
	return block;
}

std::unique_ptr<nanoapi::BlockReceiveT> nano::ipc::flatbuffers_builder::from (nano::receive_block const & block_a)
{
	auto block (std::make_unique<nanoapi::BlockReceiveT> ());
	block->hash = block_a.hash ().to_string ();
	block->source = block_a.source ().to_string ();
	block->previous = block_a.previous ().to_string ();
	block_a.signature.encode_hex (block->signature);
	block->work = nano::to_string_hex (block_a.work);
	return block;
}

std::unique_ptr<nanoapi::BlockOpenT> nano::ipc::flatbuffers_builder::from (nano::open_block const & block_a)
{
	auto block (std::make_unique<nanoapi::BlockOpenT> ());
	block->hash = block_a.hash ().to_string ();
	block->source = block_a.source ().to_string ();
	block->account = block_a.account ().to_account ();
	block->representative = block_a.representative ().to_account ();
	block_a.signature.encode_hex (block->signature);
	block->work = nano::to_string_hex (block_a.work);
	return block;
}

std::unique_ptr<nanoapi::BlockChangeT> nano::ipc::flatbuffers_builder::from (nano::change_block const & block_a)
{
	auto block (std::make_unique<nanoapi::BlockChangeT> ());
	block->hash = block_a.hash ().to_string ();
	block->previous = block_a.previous ().to_string ();
	block->representative = block_a.representative ().to_account ();
	block_a.signature.encode_hex (block->signature);
	block->work = nano::to_string_hex (block_a.work);
	return block;
}

nanoapi::BlockUnion nano::ipc::flatbuffers_builder::block_to_union (nano::block const & block_a, nano::amount const & amount_a, bool is_state_send_a)
{
	nanoapi::BlockUnion u;
	switch (block_a.type ())
	{
		case nano::block_type::state:
		{
			u.Set (*from (dynamic_cast<nano::state_block const &> (block_a), amount_a, is_state_send_a));
			break;
		}
		case nano::block_type::send:
		{
			u.Set (*from (dynamic_cast<nano::send_block const &> (block_a)));
			break;
		}
		case nano::block_type::receive:
		{
			u.Set (*from (dynamic_cast<nano::receive_block const &> (block_a)));
			break;
		}
		case nano::block_type::open:
		{
			u.Set (*from (dynamic_cast<nano::open_block const &> (block_a)));
			break;
		}
		case nano::block_type::change:
		{
			u.Set (*from (dynamic_cast<nano::change_block const &> (block_a)));
			break;
		}

		default:
			debug_assert (false);
	}
	return u;
}
