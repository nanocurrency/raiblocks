#include <nano/lib/config.hpp>
#include <nano/qt/qt.hpp>

#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <cmath>
#include <iomanip>
#include <sstream>

namespace
{
void show_line_error (QLineEdit & line)
{
	line.setStyleSheet ("QLineEdit { color: red }");
}
void show_line_ok (QLineEdit & line)
{
	line.setStyleSheet ("QLineEdit { color: black }");
}
void show_label_error (QLabel & label)
{
	label.setStyleSheet ("QLabel { color: red }");
}
void show_label_ok (QLabel & label)
{
	label.setStyleSheet ("QLabel { color: black }");
}
void show_button_error (QPushButton & button)
{
	button.setStyleSheet ("QPushButton { color: red }");
}
void show_button_ok (QPushButton & button)
{
	button.setStyleSheet ("QPushButton { color: black }");
}
void show_button_success (QPushButton & button)
{
	button.setStyleSheet ("QPushButton { color: blue }");
}
}

bool nano_qt::eventloop_processor::event (QEvent * event_a)
{
	debug_assert (dynamic_cast<nano_qt::eventloop_event *> (event_a) != nullptr);
	static_cast<nano_qt::eventloop_event *> (event_a)->action ();
	return true;
}

nano_qt::eventloop_event::eventloop_event (std::function<void ()> const & action_a) :
	QEvent (QEvent::Type::User),
	action (action_a)
{
}

nano_qt::self_pane::self_pane (nano_qt::wallet & wallet_a, nano::account const & account_a) :
	window (new QWidget),
	layout (new QVBoxLayout),
	self_layout (new QHBoxLayout),
	self_window (new QWidget),
	your_account_label (new QLabel ("Your Nano account:")),
	account_window (new QWidget),
	account_layout (new QHBoxLayout),
	account_text (new QLineEdit),
	copy_button (new QPushButton ("Copy")),
	balance_window (new QWidget),
	balance_layout (new QHBoxLayout),
	balance_label (new QLabel),
	wallet (wallet_a)
{
	your_account_label->setStyleSheet ("font-weight: bold;");
	std::string network = wallet.node.network_params.network.get_current_network_as_string ();
	if (!network.empty ())
	{
		network[0] = std::toupper (network[0]);
	}
	version = new QLabel (boost::str (boost::format ("%1% %2% network") % NANO_VERSION_STRING % network).c_str ());

	self_layout->addWidget (your_account_label);
	self_layout->addStretch ();
	self_layout->addWidget (version);
	self_layout->setContentsMargins (0, 0, 0, 0);
	self_window->setLayout (self_layout);
	account_text->setReadOnly (true);
	account_text->setStyleSheet ("QLineEdit{ background: #ddd; }");
	account_layout->addWidget (account_text, 9);
	account_layout->addWidget (copy_button, 1);
	account_layout->setContentsMargins (0, 0, 0, 0);
	account_window->setLayout (account_layout);
	layout->addWidget (self_window);
	layout->addWidget (account_window);
	balance_label->setStyleSheet ("font-weight: bold;");
	balance_layout->addWidget (balance_label);
	balance_layout->addStretch ();
	balance_layout->setContentsMargins (0, 0, 0, 0);
	balance_window->setLayout (balance_layout);
	layout->addWidget (balance_window);
	layout->setContentsMargins (5, 5, 5, 5);
	window->setLayout (layout);

	QObject::connect (copy_button, &QPushButton::clicked, [this] () {
		this->wallet.application.clipboard ()->setText (QString (this->wallet.account.to_account ().c_str ()));
		copy_button->setText ("Copied!");
		this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (2), [this] () {
			this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
				copy_button->setText ("Copy");
			}));
		});
	});
}

void nano_qt::self_pane::set_balance_text (std::pair<nano::uint128_t, nano::uint128_t> balance_a)
{
	auto final_text (std::string ("Balance: ") + wallet.format_balance (balance_a.first));
	if (!balance_a.second.is_zero ())
	{
		final_text += "\nPending: " + wallet.format_balance (balance_a.second);
	}
	wallet.self.balance_label->setText (QString (final_text.c_str ()));
}

nano_qt::accounts::accounts (nano_qt::wallet & wallet_a) :
	wallet_balance_label (new QLabel),
	window (new QWidget),
	layout (new QVBoxLayout),
	model (new QStandardItemModel),
	view (new QTableView),
	use_account (new QPushButton ("Use account")),
	create_account (new QPushButton ("Create account")),
	import_wallet (new QPushButton ("Import wallet")),
	backup_seed (new QPushButton ("Copy wallet seed to clipboard")),
	separator (new QFrame),
	account_key_line (new QLineEdit),
	account_key_button (new QPushButton ("Import adhoc key")),
	back (new QPushButton ("Back")),
	wallet (wallet_a)
{
	separator->setFrameShape (QFrame::HLine);
	separator->setFrameShadow (QFrame::Sunken);
	model->setHorizontalHeaderItem (0, new QStandardItem ("Balance"));
	model->setHorizontalHeaderItem (1, new QStandardItem ("Account"));
	view->setEditTriggers (QAbstractItemView::NoEditTriggers);
	view->setModel (model);
	view->verticalHeader ()->hide ();
	view->setContextMenuPolicy (Qt::ContextMenuPolicy::CustomContextMenu);
	view->horizontalHeader ()->setStretchLastSection (true);
	layout->addWidget (wallet_balance_label);
	layout->addWidget (view);
	layout->addWidget (use_account);
	layout->addWidget (create_account);
	layout->addWidget (import_wallet);
	layout->addWidget (backup_seed);
	layout->addWidget (separator);
	layout->addWidget (account_key_line);
	layout->addWidget (account_key_button);
	layout->addWidget (back);
	window->setLayout (layout);

	QObject::connect (use_account, &QPushButton::released, [this] () {
		auto selection (view->selectionModel ()->selection ().indexes ());
		if (selection.size () == 1)
		{
			auto error (this->wallet.account.decode_account (model->item (selection[0].row (), 1)->text ().toStdString ()));
			(void)error;
			debug_assert (!error);
			this->wallet.refresh ();
		}
	});
	QObject::connect (account_key_button, &QPushButton::released, [this] () {
		QString key_text_wide (account_key_line->text ());
		std::string key_text (key_text_wide.toLocal8Bit ());
		nano::raw_key key;
		if (!key.decode_hex (key_text))
		{
			show_line_ok (*account_key_line);
			account_key_line->clear ();
			this->wallet.wallet_m->insert_adhoc (key);
			this->wallet.accounts.refresh ();
			this->wallet.accounts.refresh_wallet_balance ();
			this->wallet.history.refresh ();
		}
		else
		{
			show_line_error (*account_key_line);
		}
	});
	QObject::connect (back, &QPushButton::clicked, [this] () {
		this->wallet.pop_main_stack ();
	});
	QObject::connect (create_account, &QPushButton::released, [this] () {
		{
			auto transaction (this->wallet.wallet_m->wallets.tx_begin_write ());
			if (this->wallet.wallet_m->store.valid_password (transaction))
			{
				this->wallet.wallet_m->deterministic_insert (transaction);
				show_button_success (*create_account);
				create_account->setText ("New account was created");
				this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
					this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
						show_button_ok (*create_account);
						create_account->setText ("Create account");
					}));
				});
			}
			else
			{
				show_button_error (*create_account);
				create_account->setText ("Wallet is locked, unlock it to create account");
				this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
					this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
						show_button_ok (*create_account);
						create_account->setText ("Create account");
					}));
				});
			}
		}
		refresh ();
	});
	QObject::connect (import_wallet, &QPushButton::released, [this] () {
		this->wallet.push_main_stack (this->wallet.import.window);
	});
	QObject::connect (backup_seed, &QPushButton::released, [this] () {
		nano::raw_key seed;
		auto transaction (this->wallet.wallet_m->wallets.tx_begin_read ());
		if (this->wallet.wallet_m->store.valid_password (transaction))
		{
			this->wallet.wallet_m->store.seed (seed, transaction);
			this->wallet.application.clipboard ()->setText (QString (seed.to_string ().c_str ()));
			show_button_success (*backup_seed);
			backup_seed->setText ("Seed was copied to clipboard");
			this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
				this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
					show_button_ok (*backup_seed);
					backup_seed->setText ("Copy wallet seed to clipboard");
				}));
			});
		}
		else
		{
			this->wallet.application.clipboard ()->setText ("");
			show_button_error (*backup_seed);
			backup_seed->setText ("Wallet is locked, unlock it to enable the backup");
			this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
				this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
					show_button_ok (*backup_seed);
					backup_seed->setText ("Copy wallet seed to clipboard");
				}));
			});
		}
	});
	QObject::connect (account_key_line, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = account_key_line->cursorPosition ();
		account_key_line->setText (value.trimmed ());
		account_key_line->setCursorPosition (pos);
	});
	refresh_wallet_balance ();
}

void nano_qt::accounts::refresh_wallet_balance ()
{
	auto transaction (this->wallet.wallet_m->wallets.tx_begin_read ());
	auto block_transaction (this->wallet.node.store.tx_begin_read ());
	nano::uint128_t balance (0);
	nano::uint128_t pending (0);
	for (auto i (this->wallet.wallet_m->store.begin (transaction)), j (this->wallet.wallet_m->store.end ()); i != j; ++i)
	{
		nano::public_key const & key (i->first);
		balance = balance + (this->wallet.node.ledger.account_balance (block_transaction, key));
		pending = pending + (this->wallet.node.ledger.account_pending (block_transaction, key));
	}
	auto final_text (std::string ("Balance: ") + wallet.format_balance (balance));
	if (!pending.is_zero ())
	{
		final_text += "\nPending: " + wallet.format_balance (pending);
	}
	wallet_balance_label->setText (QString (final_text.c_str ()));
	this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (60), [this] () {
		this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
			refresh_wallet_balance ();
		}));
	});
}

void nano_qt::accounts::refresh ()
{
	model->removeRows (0, model->rowCount ());
	auto transaction (wallet.wallet_m->wallets.tx_begin_read ());
	auto block_transaction (this->wallet.node.store.tx_begin_read ());
	QBrush brush;
	for (auto i (wallet.wallet_m->store.begin (transaction)), j (wallet.wallet_m->store.end ()); i != j; ++i)
	{
		nano::public_key key (i->first);
		auto balance_amount (wallet.node.ledger.account_balance (block_transaction, key));
		bool display (true);
		switch (wallet.wallet_m->store.key_type (i->second))
		{
			case nano::key_type::adhoc:
			{
				brush.setColor ("red");
				display = !balance_amount.is_zero ();
				break;
			}
			default:
			{
				brush.setColor ("black");
				break;
			}
		}
		if (display)
		{
			QList<QStandardItem *> items;
			std::string balance = wallet.format_balance (balance_amount);
			items.push_back (new QStandardItem (balance.c_str ()));
			auto account (new QStandardItem (QString (key.to_account ().c_str ())));
			account->setForeground (brush);
			items.push_back (account);
			model->appendRow (items);
		}
	}
}

nano_qt::import::import (nano_qt::wallet & wallet_a) :
	window (new QWidget),
	layout (new QVBoxLayout),
	seed_label (new QLabel ("Seed:")),
	seed (new QLineEdit),
	clear_label (new QLabel ("Modifying seed clears existing keys\nType 'clear keys' below to confirm:")),
	clear_line (new QLineEdit),
	import_seed (new QPushButton ("Import seed")),
	separator (new QFrame),
	filename_label (new QLabel ("Path to file:")),
	filename (new QLineEdit),
	password_label (new QLabel ("Password:")),
	password (new QLineEdit),
	perform (new QPushButton ("Import")),
	back (new QPushButton ("Back")),
	wallet (wallet_a)
{
	layout->addWidget (seed_label);
	layout->addWidget (seed);
	layout->addWidget (clear_label);
	layout->addWidget (clear_line);
	clear_line->setPlaceholderText ("clear keys");
	layout->addWidget (import_seed);
	layout->addWidget (separator);
	layout->addWidget (filename_label);
	layout->addWidget (filename);
	layout->addWidget (password_label);
	layout->addWidget (password);
	layout->addWidget (perform);
	layout->addStretch ();
	layout->addWidget (back);
	window->setLayout (layout);
	QObject::connect (perform, &QPushButton::released, [this] () {
		std::ifstream stream;
		stream.open (filename->text ().toStdString ().c_str ());
		if (!stream.fail ())
		{
			show_line_ok (*filename);
			std::stringstream contents;
			contents << stream.rdbuf ();
			if (!this->wallet.wallet_m->import (contents.str (), password->text ().toStdString ().c_str ()))
			{
				show_line_ok (*password);
				this->wallet.accounts.refresh ();
				password->clear ();
				filename->clear ();
			}
			else
			{
				show_line_error (*password);
			}
		}
		else
		{
			show_line_error (*filename);
		}
	});
	QObject::connect (back, &QPushButton::released, [this] () {
		this->wallet.pop_main_stack ();
	});
	QObject::connect (import_seed, &QPushButton::released, [this] () {
		if (clear_line->text ().toStdString () == "clear keys")
		{
			show_line_ok (*clear_line);
			nano::raw_key seed_l;
			if (!seed_l.decode_hex (seed->text ().toStdString ()))
			{
				bool successful (false);
				{
					auto transaction (this->wallet.wallet_m->wallets.tx_begin_write ());
					if (this->wallet.wallet_m->store.valid_password (transaction))
					{
						this->wallet.account = this->wallet.wallet_m->change_seed (transaction, seed_l);
						successful = true;
						// Pending check for accounts to restore if bootstrap is in progress
						if (this->wallet.node.bootstrap_initiator.in_progress ())
						{
							this->wallet.needs_deterministic_restore = true;
						}
					}
					else
					{
						show_line_error (*seed);
						show_button_error (*import_seed);
						import_seed->setText ("Wallet is locked, unlock it to enable the import");
						this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (10), [this] () {
							this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
								show_line_ok (*seed);
								show_button_ok (*import_seed);
								import_seed->setText ("Import seed");
							}));
						});
					}
				}
				if (successful)
				{
					seed->clear ();
					clear_line->clear ();
					show_line_ok (*seed);
					show_button_success (*import_seed);
					import_seed->setText ("Successful import of seed");
					this->wallet.refresh ();
					this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
						this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
							show_button_ok (*import_seed);
							import_seed->setText ("Import seed");
						}));
					});
				}
			}
			else
			{
				show_line_error (*seed);
				show_button_error (*import_seed);
				if (seed->text ().toStdString ().size () != 64)
				{
					import_seed->setText ("Incorrect seed, length must be 64");
				}
				else
				{
					import_seed->setText ("Incorrect seed. Only HEX characters allowed");
				}
				this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
					this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
						show_button_ok (*import_seed);
						import_seed->setText ("Import seed");
					}));
				});
			}
		}
		else
		{
			show_line_error (*clear_line);
			show_button_error (*import_seed);
			import_seed->setText ("Type words 'clear keys'");
			this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
				this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
					show_button_ok (*import_seed);
					import_seed->setText ("Import seed");
				}));
			});
		}
	});
	QObject::connect (seed, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = seed->cursorPosition ();
		seed->setText (value.trimmed ());
		seed->setCursorPosition (pos);
	});
	QObject::connect (filename, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = filename->cursorPosition ();
		filename->setText (value.trimmed ());
		filename->setCursorPosition (pos);
	});
}

nano_qt::history::history (nano::ledger & ledger_a, nano::account const & account_a, nano_qt::wallet & wallet_a) :
	window (new QWidget),
	layout (new QVBoxLayout),
	model (new QStandardItemModel),
	view (new QTableView),
	tx_window (new QWidget),
	tx_layout (new QHBoxLayout),
	tx_label (new QLabel ("Account history count:")),
	tx_count (new QSpinBox),
	ledger (ledger_a),
	account (account_a),
	wallet (wallet_a)
{ /*
	tx_count->setRange (1, 256);
	tx_layout->addWidget (tx_label);
	tx_layout->addWidget (tx_count);
	tx_layout->setContentsMargins (0, 0, 0, 0);
	tx_window->setLayout (tx_layout);*/
	model->setHorizontalHeaderItem (0, new QStandardItem ("Type"));
	model->setHorizontalHeaderItem (1, new QStandardItem ("Account"));
	model->setHorizontalHeaderItem (2, new QStandardItem ("Amount"));
	model->setHorizontalHeaderItem (3, new QStandardItem ("Hash"));
	view->setModel (model);
	view->setEditTriggers (QAbstractItemView::NoEditTriggers);
	view->verticalHeader ()->hide ();
	view->horizontalHeader ()->setStretchLastSection (true);
	//	layout->addWidget (tx_window);
	layout->addWidget (view);
	layout->setContentsMargins (0, 0, 0, 0);
	window->setLayout (layout);
	tx_count->setValue (32);
}

namespace
{
class short_text_visitor : public nano::block_visitor
{
public:
	short_text_visitor (nano::transaction const & transaction_a, nano::ledger & ledger_a) :
		transaction (transaction_a),
		ledger (ledger_a)
	{
	}
	void send_block (nano::send_block const & block_a)
	{
		type = "Send";
		account = block_a.hashables.destination;
		bool error_or_pruned (false);
		amount = ledger.amount_safe (transaction, block_a.hash (), error_or_pruned);
		if (error_or_pruned)
		{
			type = "Send (pruned)";
		}
	}
	void receive_block (nano::receive_block const & block_a)
	{
		type = "Receive";
		bool error_or_pruned (false);
		account = ledger.account_safe (transaction, block_a.hashables.source, error_or_pruned);
		amount = ledger.amount_safe (transaction, block_a.hash (), error_or_pruned);
		if (error_or_pruned)
		{
			type = "Receive (pruned)";
		}
	}
	void open_block (nano::open_block const & block_a)
	{
		static nano::network_params params;
		type = "Receive";
		if (block_a.hashables.source != params.ledger.genesis_account ())
		{
			bool error_or_pruned (false);
			account = ledger.account_safe (transaction, block_a.hashables.source, error_or_pruned);
			amount = ledger.amount_safe (transaction, block_a.hash (), error_or_pruned);
			if (error_or_pruned)
			{
				type = "Receive (pruned)";
			}
		}
		else
		{
			account = params.ledger.genesis_account ();
			amount = nano::dev::genesis_amount;
		}
	}
	void change_block (nano::change_block const & block_a)
	{
		type = "Change";
		amount = 0;
		account = block_a.hashables.representative;
	}
	void state_block (nano::state_block const & block_a)
	{
		auto balance (block_a.hashables.balance.number ());
		bool error_or_pruned (false);
		auto previous_balance (ledger.balance_safe (transaction, block_a.hashables.previous, error_or_pruned));
		if (error_or_pruned)
		{
			type = "Unknown (pruned)";
			amount = 0;
			account = block_a.hashables.account;
		}
		else if (balance < previous_balance)
		{
			type = "Send";
			amount = previous_balance - balance;
			account = block_a.hashables.link.as_account ();
		}
		else
		{
			if (block_a.hashables.link.is_zero ())
			{
				type = "Change";
				account = block_a.hashables.representative;
			}
			else if (balance == previous_balance && ledger.is_epoch_link (block_a.hashables.link))
			{
				type = "Epoch";
				account = ledger.epoch_signer (block_a.hashables.link);
			}
			else
			{
				type = "Receive";
				std::cerr << block_a.hash ().to_string () << ':' << block_a.hashables.link.as_block_hash ().to_string () << '\n';
				debug_assert (ledger.store.block.exists (transaction, block_a.hashables.link.as_block_hash ()));
				account = ledger.account_safe (transaction, block_a.hashables.link.as_block_hash (), error_or_pruned);
				if (error_or_pruned)
				{
					type = "Receive (pruned)";
				}
			}
			amount = balance - previous_balance;
		}
	}
	nano::transaction const & transaction;
	nano::ledger & ledger;
	std::string type;
	nano::uint128_t amount;
	nano::account account;
};
}

void nano_qt::history::refresh ()
{
	auto transaction (ledger.store.tx_begin_read ());
	model->removeRows (0, model->rowCount ());
	auto hash (ledger.latest (transaction, account));
	short_text_visitor visitor (transaction, ledger);
	for (auto i (0), n (tx_count->value ()); i < n && !hash.is_zero (); ++i)
	{
		QList<QStandardItem *> items;
		auto block (ledger.store.block.get (transaction, hash));
		if (block != nullptr)
		{
			block->visit (visitor);
			items.push_back (new QStandardItem (QString (visitor.type.c_str ())));
			items.push_back (new QStandardItem (QString (visitor.account.to_account ().c_str ())));
			auto balanceItem = new QStandardItem (QString (wallet.format_balance (visitor.amount).c_str ()));
			balanceItem->setData (Qt::AlignRight, Qt::TextAlignmentRole);
			items.push_back (balanceItem);
			items.push_back (new QStandardItem (QString (hash.to_string ().c_str ())));
			hash = block->previous ();
			model->appendRow (items);
		}
	}
}

nano_qt::block_viewer::block_viewer (nano_qt::wallet & wallet_a) :
	window (new QWidget),
	layout (new QVBoxLayout),
	hash_label (new QLabel ("Hash:")),
	hash (new QLineEdit),
	block_label (new QLabel ("Block:")),
	block (new QPlainTextEdit),
	successor_label (new QLabel ("Successor:")),
	successor (new QLineEdit),
	retrieve (new QPushButton ("Retrieve")),
	rebroadcast (new QPushButton ("Rebroadcast")),
	back (new QPushButton ("Back")),
	wallet (wallet_a)
{
	layout->addWidget (hash_label);
	layout->addWidget (hash);
	layout->addWidget (block_label);
	layout->addWidget (block);
	layout->addWidget (successor_label);
	layout->addWidget (successor);
	layout->addWidget (retrieve);
	layout->addWidget (rebroadcast);
	layout->addStretch ();
	layout->addWidget (back);
	window->setLayout (layout);
	QObject::connect (back, &QPushButton::released, [this] () {
		this->wallet.pop_main_stack ();
	});
	QObject::connect (retrieve, &QPushButton::released, [this] () {
		nano::block_hash hash_l;
		if (!hash_l.decode_hex (hash->text ().toStdString ()))
		{
			auto transaction (this->wallet.node.store.tx_begin_read ());
			auto block_l (this->wallet.node.store.block.get (transaction, hash_l));
			if (block_l != nullptr)
			{
				std::string contents;
				block_l->serialize_json (contents);
				block->setPlainText (contents.c_str ());
				auto successor_l (this->wallet.node.store.block.successor (transaction, hash_l));
				successor->setText (successor_l.to_string ().c_str ());
			}
			else
			{
				block->setPlainText ("Block not found");
			}
		}
		else
		{
			block->setPlainText ("Bad block hash");
		}
	});
	QObject::connect (rebroadcast, &QPushButton::released, [this] () {
		nano::block_hash block;
		auto error (block.decode_hex (hash->text ().toStdString ()));
		if (!error)
		{
			auto transaction (this->wallet.node.store.tx_begin_read ());
			if (this->wallet.node.store.block.exists (transaction, block))
			{
				rebroadcast->setEnabled (false);
				this->wallet.node.background ([this, block] () {
					rebroadcast_action (block);
				});
			}
		}
	});
	QObject::connect (hash, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = hash->cursorPosition ();
		hash->setText (value.trimmed ());
		hash->setCursorPosition (pos);
	});
	rebroadcast->setToolTip ("Rebroadcast block into the network");
}

void nano_qt::block_viewer::rebroadcast_action (nano::block_hash const & hash_a)
{
	auto done (true);
	auto transaction (wallet.node.ledger.store.tx_begin_read ());
	auto block (wallet.node.store.block.get (transaction, hash_a));
	if (block != nullptr)
	{
		wallet.node.network.flood_block (block);
		auto successor (wallet.node.store.block.successor (transaction, hash_a));
		if (!successor.is_zero ())
		{
			done = false;
			wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (1), [this, successor] () {
				this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this, successor] () {
					rebroadcast_action (successor);
				}));
			});
		}
	}
	if (done)
	{
		rebroadcast->setEnabled (true);
	}
}

nano_qt::account_viewer::account_viewer (nano_qt::wallet & wallet_a) :
	window (new QWidget),
	layout (new QVBoxLayout),
	account_label (new QLabel ("Account:")),
	account_line (new QLineEdit),
	refresh (new QPushButton ("Refresh")),
	balance_window (new QWidget),
	balance_layout (new QHBoxLayout),
	balance_label (new QLabel),
	history (wallet_a.node.ledger, account, wallet_a),
	back (new QPushButton ("Back")),
	account (wallet_a.account),
	wallet (wallet_a)
{
	layout->addWidget (account_label);
	layout->addWidget (account_line);
	layout->addWidget (refresh);
	balance_layout->addWidget (balance_label);
	balance_layout->addStretch ();
	balance_layout->setContentsMargins (0, 0, 0, 0);
	balance_window->setLayout (balance_layout);
	layout->addWidget (balance_window);
	layout->addWidget (history.window);
	layout->addWidget (back);
	window->setLayout (layout);
	QObject::connect (back, &QPushButton::released, [this] () {
		this->wallet.pop_main_stack ();
	});
	QObject::connect (refresh, &QPushButton::released, [this] () {
		account.clear ();
		if (!account.decode_account (account_line->text ().toStdString ()))
		{
			show_line_ok (*account_line);
			this->history.refresh ();
			auto balance (this->wallet.node.balance_pending (account, false));
			auto final_text (std::string ("Balance (NANO): ") + wallet.format_balance (balance.first));
			if (!balance.second.is_zero ())
			{
				final_text += "\nPending: " + wallet.format_balance (balance.second);
			}
			balance_label->setText (QString (final_text.c_str ()));
		}
		else
		{
			show_line_error (*account_line);
			balance_label->clear ();
		}
	});
	QObject::connect (account_line, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = account_line->cursorPosition ();
		account_line->setText (value.trimmed ());
		account_line->setCursorPosition (pos);
	});
}

nano_qt::stats_viewer::stats_viewer (nano_qt::wallet & wallet_a) :
	window (new QWidget),
	layout (new QVBoxLayout),
	refresh (new QPushButton ("Refresh")),
	clear (new QPushButton ("Clear Statistics")),
	model (new QStandardItemModel),
	view (new QTableView),
	back (new QPushButton ("Back")),
	wallet (wallet_a)
{
	model->setHorizontalHeaderItem (0, new QStandardItem ("Last updated"));
	model->setHorizontalHeaderItem (1, new QStandardItem ("Type"));
	model->setHorizontalHeaderItem (2, new QStandardItem ("Detail"));
	model->setHorizontalHeaderItem (3, new QStandardItem ("Direction"));
	model->setHorizontalHeaderItem (4, new QStandardItem ("Value"));
	view->setModel (model);
	view->setEditTriggers (QAbstractItemView::NoEditTriggers);
	view->verticalHeader ()->hide ();
	view->horizontalHeader ()->setStretchLastSection (true);
	layout->setContentsMargins (0, 0, 0, 0);
	layout->addWidget (view);
	layout->addWidget (refresh);
	layout->addWidget (clear);
	layout->addWidget (back);
	window->setLayout (layout);

	QObject::connect (back, &QPushButton::released, [this] () {
		this->wallet.pop_main_stack ();
	});
	QObject::connect (refresh, &QPushButton::released, [this] () {
		refresh_stats ();
	});

	QObject::connect (clear, &QPushButton::released, [this] () {
		this->wallet.node.stats.clear ();
		refresh_stats ();
	});

	refresh_stats ();
}

void nano_qt::stats_viewer::refresh_stats ()
{
	model->removeRows (0, model->rowCount ());

	auto sink = wallet.node.stats.log_sink_json ();
	wallet.node.stats.log_counters (*sink);
	auto json = static_cast<boost::property_tree::ptree *> (sink->to_object ());
	if (json)
	{
		// Format the stat data to make totals and values easier to read
		BOOST_FOREACH (const boost::property_tree::ptree::value_type & child, json->get_child ("entries"))
		{
			auto time = child.second.get<std::string> ("time");
			auto type = child.second.get<std::string> ("type");
			auto detail = child.second.get<std::string> ("detail");
			auto dir = child.second.get<std::string> ("dir");
			auto value = child.second.get<std::string> ("value", "0");

			if (detail == "all")
			{
				detail = "total";
			}

			if (type == "traffic_udp" || type == "traffic_tcp")
			{
				const std::vector<std::string> units = { " bytes", " KB", " MB", " GB", " TB", " PB" };
				double bytes = std::stod (value);
				auto index = bytes == 0 ? 0 : std::min (units.size () - 1, static_cast<size_t> (std::floor (std::log2 (bytes) / 10)));
				std::string unit = units[index];
				bytes /= std::pow (1024, index);

				// Only show decimals from MB and up
				int precision = index < 2 ? 0 : 2;
				std::stringstream numstream;
				numstream << std::fixed << std::setprecision (precision) << bytes;
				value = numstream.str () + unit;
			}

			QList<QStandardItem *> items;
			items.push_back (new QStandardItem (QString (time.c_str ())));
			items.push_back (new QStandardItem (QString (type.c_str ())));
			items.push_back (new QStandardItem (QString (detail.c_str ())));
			items.push_back (new QStandardItem (QString (dir.c_str ())));
			items.push_back (new QStandardItem (QString (value.c_str ())));

			model->appendRow (items);
		}
	}
}

nano_qt::status::status (nano_qt::wallet & wallet_a) :
	wallet (wallet_a)
{
	wallet.status->setToolTip ("Wallet status, block count (blocks downloaded)");
	active.insert (nano_qt::status_types::nominal);
	set_text ();
}

void nano_qt::status::erase (nano_qt::status_types status_a)
{
	debug_assert (status_a != nano_qt::status_types::nominal);
	auto erased (active.erase (status_a));
	(void)erased;
	set_text ();
}

void nano_qt::status::insert (nano_qt::status_types status_a)
{
	debug_assert (status_a != nano_qt::status_types::nominal);
	active.insert (status_a);
	set_text ();
}

void nano_qt::status::set_text ()
{
	wallet.status->setText (text ().c_str ());
	wallet.status->setStyleSheet ((std::string ("QLabel {") + color () + "}").c_str ());
}

std::string nano_qt::status::text ()
{
	debug_assert (!active.empty ());
	std::string result;
	size_t unchecked (0);
	std::string count_string;
	{
		auto size (wallet.wallet_m->wallets.node.ledger.cache.block_count.load ());
		unchecked = wallet.wallet_m->wallets.node.store.unchecked.count (wallet.wallet_m->wallets.node.store.tx_begin_read ());
		count_string = std::to_string (size);
	}

	switch (*active.begin ())
	{
		case nano_qt::status_types::disconnected:
			result = "Status: Disconnected";
			break;
		case nano_qt::status_types::working:
			result = "Status: Generating proof of work";
			break;
		case nano_qt::status_types::synchronizing:
			result = "Status: Synchronizing";
			break;
		case nano_qt::status_types::locked:
			result = "Status: Wallet locked";
			break;
		case nano_qt::status_types::vulnerable:
			result = "Status: Wallet password empty";
			break;
		case nano_qt::status_types::active:
			result = "Status: Wallet active";
			break;
		case nano_qt::status_types::nominal:
			result = "Status: Running";
			break;
		default:
			debug_assert (false);
			break;
	}

	result += ", Blocks: ";
	if (unchecked != 0 && wallet.node.bootstrap_initiator.in_progress ())
	{
		count_string += ", Queued: " + std::to_string (unchecked);
	}
	result += count_string.c_str ();

	return result;
}

std::string nano_qt::status::color ()
{
	debug_assert (!active.empty ());
	std::string result;
	switch (*active.begin ())
	{
		case nano_qt::status_types::disconnected:
			result = "color: red";
			break;
		case nano_qt::status_types::working:
			result = "color: blue";
			break;
		case nano_qt::status_types::synchronizing:
			result = "color: blue";
			break;
		case nano_qt::status_types::locked:
			result = "color: orange";
			break;
		case nano_qt::status_types::vulnerable:
			result = "color: blue";
			break;
		case nano_qt::status_types::active:
			result = "color: black";
			break;
		case nano_qt::status_types::nominal:
			result = "color: black";
			break;
		default:
			debug_assert (false);
			break;
	}
	return result;
}

nano_qt::wallet::wallet (QApplication & application_a, nano_qt::eventloop_processor & processor_a, nano::node & node_a, std::shared_ptr<nano::wallet> const & wallet_a, nano::account & account_a) :
	rendering_ratio (nano::Mxrb_ratio),
	node (node_a),
	wallet_m (wallet_a),
	account (account_a),
	processor (processor_a),
	history (node.ledger, account, *this),
	accounts (*this),
	self (*this, account_a),
	settings (*this),
	advanced (*this),
	block_creation (*this),
	block_entry (*this),
	block_viewer (*this),
	account_viewer (*this),
	stats_viewer (*this),
	import (*this),
	application (application_a),
	status (new QLabel),
	main_stack (new QStackedWidget),
	client_window (new QWidget),
	client_layout (new QVBoxLayout),
	entry_window (new QWidget),
	entry_window_layout (new QVBoxLayout),
	separator (new QFrame),
	account_history_label (new QLabel ("Account history:")),
	send_blocks (new QPushButton ("Send")),
	settings_button (new QPushButton ("Settings")),
	accounts_button (new QPushButton ("Accounts")),
	show_advanced (new QPushButton ("Advanced")),
	send_blocks_window (new QWidget),
	send_blocks_layout (new QVBoxLayout),
	send_account_label (new QLabel ("Destination account:")),
	send_account (new QLineEdit),
	send_count_label (new QLabel ("Amount:")),
	send_count (new QLineEdit),
	send_blocks_send (new QPushButton ("Send")),
	send_blocks_back (new QPushButton ("Back")),
	active_status (*this),
	needs_deterministic_restore (false)
{
	update_connected ();
	empty_password ();
	settings.update_locked (true, true);
	send_blocks_layout->addWidget (send_account_label);
	send_account->setPlaceholderText (node.network_params.ledger.zero_key.pub.to_account ().c_str ());
	send_blocks_layout->addWidget (send_account);
	send_blocks_layout->addWidget (send_count_label);
	send_count->setPlaceholderText ("0");
	send_blocks_layout->addWidget (send_count);
	send_blocks_layout->addWidget (send_blocks_send);
	send_blocks_layout->addStretch ();
	send_blocks_layout->addWidget (send_blocks_back);
	send_blocks_layout->setContentsMargins (0, 0, 0, 0);
	send_blocks_window->setLayout (send_blocks_layout);

	entry_window_layout->addWidget (account_history_label);
	entry_window_layout->addWidget (history.window);
	entry_window_layout->addWidget (send_blocks);
	entry_window_layout->addWidget (settings_button);
	entry_window_layout->addWidget (accounts_button);
	entry_window_layout->addWidget (show_advanced);
	entry_window_layout->setContentsMargins (0, 0, 0, 0);
	entry_window_layout->setSpacing (5);
	entry_window->setLayout (entry_window_layout);

	main_stack->addWidget (entry_window);
	status->setContentsMargins (5, 5, 5, 5);
	status->setAlignment (Qt::AlignHCenter);
	separator->setFrameShape (QFrame::HLine);
	separator->setFrameShadow (QFrame::Sunken);

	client_layout->addWidget (status);
	client_layout->addWidget (self.window);
	client_layout->addWidget (separator);
	client_layout->addWidget (main_stack);
	client_layout->setSpacing (0);
	client_layout->setContentsMargins (0, 0, 0, 0);
	client_window->setLayout (client_layout);
	client_window->resize (620, 640);
	client_window->setStyleSheet ("\
		QLineEdit { padding: 3px; } \
	");
	QObject::connect (send_account, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = send_account->cursorPosition ();
		send_account->setText (value.trimmed ());
		send_account->setCursorPosition (pos);
	});
	QObject::connect (send_count, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = send_count->cursorPosition ();
		send_count->setText (value.trimmed ());
		send_count->setCursorPosition (pos);
	});
	refresh ();
}

void nano_qt::wallet::ongoing_refresh ()
{
	std::weak_ptr<nano_qt::wallet> wallet_w (shared_from_this ());

	// Update balance if needed. This happens on an alarm thread, which posts back to the UI
	// to do the actual rendering. This avoid UI lockups as balance_pending may take several
	// seconds if there's a lot of pending transactions.
	if (needs_balance_refresh)
	{
		needs_balance_refresh = false;
		auto balance_l (node.balance_pending (account, false));
		application.postEvent (&processor, new eventloop_event ([wallet_w, balance_l] () {
			if (auto this_l = wallet_w.lock ())
			{
				this_l->self.set_balance_text (balance_l);
			}
		}));
	}

	// Updates the status line periodically with bootstrap status and block counts.
	application.postEvent (&processor, new eventloop_event ([wallet_w] () {
		if (auto this_l = wallet_w.lock ())
		{
			this_l->active_status.set_text ();
		}
	}));

	node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [wallet_w] () {
		if (auto wallet_l = wallet_w.lock ())
		{
			wallet_l->ongoing_refresh ();
		}
	});
}

void nano_qt::wallet::start ()
{
	ongoing_refresh ();
	std::weak_ptr<nano_qt::wallet> this_w (shared_from_this ());
	QObject::connect (settings_button, &QPushButton::released, [this_w] () {
		if (auto this_l = this_w.lock ())
		{
			this_l->settings.activate ();
		}
	});
	QObject::connect (accounts_button, &QPushButton::released, [this_w] () {
		if (auto this_l = this_w.lock ())
		{
			this_l->push_main_stack (this_l->accounts.window);
		}
	});
	QObject::connect (show_advanced, &QPushButton::released, [this_w] () {
		if (auto this_l = this_w.lock ())
		{
			this_l->push_main_stack (this_l->advanced.window);
		}
	});
	QObject::connect (send_blocks_send, &QPushButton::released, [this_w] () {
		if (auto this_l = this_w.lock ())
		{
			show_line_ok (*this_l->send_count);
			show_line_ok (*this_l->send_account);
			nano::amount amount;
			if (!amount.decode_dec (this_l->send_count->text ().toStdString (), this_l->rendering_ratio))
			{
				nano::uint128_t actual (amount.number ());
				QString account_text (this_l->send_account->text ());
				std::string account_text_narrow (account_text.toLocal8Bit ());
				nano::account account_l;
				auto parse_error (account_l.decode_account (account_text_narrow));
				if (!parse_error)
				{
					auto balance (this_l->node.balance (this_l->account));
					if (actual <= balance)
					{
						auto transaction (this_l->wallet_m->wallets.tx_begin_read ());
						if (this_l->wallet_m->store.valid_password (transaction))
						{
							this_l->send_blocks_send->setEnabled (false);
							this_l->node.background ([this_w, account_l, actual] () {
								if (auto this_l = this_w.lock ())
								{
									this_l->wallet_m->send_async (this_l->account, account_l, actual, [this_w] (std::shared_ptr<nano::block> const & block_a) {
										if (auto this_l = this_w.lock ())
										{
											auto succeeded (block_a != nullptr);
											this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w, succeeded] () {
												if (auto this_l = this_w.lock ())
												{
													this_l->send_blocks_send->setEnabled (true);
													if (succeeded)
													{
														this_l->send_count->clear ();
														this_l->send_account->clear ();
														this_l->accounts.refresh ();
													}
													else
													{
														show_line_error (*this_l->send_count);
													}
												}
											}));
										}
									});
								}
							});
						}
						else
						{
							show_button_error (*this_l->send_blocks_send);
							this_l->send_blocks_send->setText ("Wallet is locked, unlock it to send");
							this_l->node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this_w] () {
								if (auto this_l = this_w.lock ())
								{
									this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w] () {
										if (auto this_l = this_w.lock ())
										{
											show_button_ok (*this_l->send_blocks_send);
											this_l->send_blocks_send->setText ("Send");
										}
									}));
								}
							});
						}
					}
					else
					{
						show_line_error (*this_l->send_count);
						show_button_error (*this_l->send_blocks_send);
						this_l->send_blocks_send->setText ("Not enough balance");
						this_l->node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this_w] () {
							if (auto this_l = this_w.lock ())
							{
								this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w] () {
									if (auto this_l = this_w.lock ())
									{
										show_button_ok (*this_l->send_blocks_send);
										this_l->send_blocks_send->setText ("Send");
									}
								}));
							}
						});
					}
				}
				else
				{
					show_line_error (*this_l->send_account);
					show_button_error (*this_l->send_blocks_send);
					this_l->send_blocks_send->setText ("Bad destination account");
					this_l->node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this_w] () {
						if (auto this_l = this_w.lock ())
						{
							this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w] () {
								if (auto this_l = this_w.lock ())
								{
									show_button_ok (*this_l->send_blocks_send);
									this_l->send_blocks_send->setText ("Send");
								}
							}));
						}
					});
				}
			}
			else
			{
				show_line_error (*this_l->send_count);
				show_button_error (*this_l->send_blocks_send);
				this_l->send_blocks_send->setText ("Bad amount number");
				this_l->node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this_w] () {
					if (auto this_l = this_w.lock ())
					{
						this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w] () {
							if (auto this_l = this_w.lock ())
							{
								show_button_ok (*this_l->send_blocks_send);
								this_l->send_blocks_send->setText ("Send");
							}
						}));
					}
				});
			}
		}
	});
	QObject::connect (send_blocks_back, &QPushButton::released, [this_w] () {
		if (auto this_l = this_w.lock ())
		{
			this_l->pop_main_stack ();
		}
	});
	QObject::connect (send_blocks, &QPushButton::released, [this_w] () {
		if (auto this_l = this_w.lock ())
		{
			this_l->push_main_stack (this_l->send_blocks_window);
		}
	});
	node.observers.blocks.add ([this_w] (nano::election_status const & status_a, std::vector<nano::vote_with_weight_info> const & votes_a, nano::account const & account_a, nano::uint128_t const & amount_a, bool) {
		if (auto this_l = this_w.lock ())
		{
			this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w, status_a, account_a] () {
				if (auto this_l = this_w.lock ())
				{
					if (this_l->wallet_m->exists (account_a))
					{
						this_l->accounts.refresh ();
					}
					if (account_a == this_l->account)
					{
						this_l->history.refresh ();
					}
				}
			}));
		}
	});
	node.observers.account_balance.add ([this_w] (nano::account const & account_a, bool is_pending) {
		if (auto this_l = this_w.lock ())
		{
			this_l->needs_balance_refresh = this_l->needs_balance_refresh || account_a == this_l->account;
		}
	});
	node.observers.wallet.add ([this_w] (bool active_a) {
		if (auto this_l = this_w.lock ())
		{
			this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w, active_a] () {
				if (auto this_l = this_w.lock ())
				{
					if (active_a)
					{
						this_l->active_status.insert (nano_qt::status_types::active);
					}
					else
					{
						this_l->active_status.erase (nano_qt::status_types::active);
					}
				}
			}));
		}
	});
	node.observers.endpoint.add ([this_w] (std::shared_ptr<nano::transport::channel> const &) {
		if (auto this_l = this_w.lock ())
		{
			this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w] () {
				if (auto this_l = this_w.lock ())
				{
					this_l->update_connected ();
				}
			}));
		}
	});
	node.observers.disconnect.add ([this_w] () {
		if (auto this_l = this_w.lock ())
		{
			this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w] () {
				if (auto this_l = this_w.lock ())
				{
					this_l->update_connected ();
				}
			}));
		}
	});
	node.bootstrap_initiator.add_observer ([this_w] (bool active_a) {
		if (auto this_l = this_w.lock ())
		{
			this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w, active_a] () {
				if (auto this_l = this_w.lock ())
				{
					if (active_a)
					{
						this_l->active_status.insert (nano_qt::status_types::synchronizing);
					}
					else
					{
						this_l->active_status.erase (nano_qt::status_types::synchronizing);
						// Check for accounts to restore
						if (this_l->needs_deterministic_restore)
						{
							this_l->needs_deterministic_restore = false;
							auto transaction (this_l->wallet_m->wallets.tx_begin_write ());
							this_l->wallet_m->deterministic_restore (transaction);
						}
					}
				}
			}));
		}
	});
	node.work.work_observers.add ([this_w] (bool working) {
		if (auto this_l = this_w.lock ())
		{
			this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w, working] () {
				if (auto this_l = this_w.lock ())
				{
					if (working)
					{
						this_l->active_status.insert (nano_qt::status_types::working);
					}
					else
					{
						this_l->active_status.erase (nano_qt::status_types::working);
					}
				}
			}));
		}
	});
	wallet_m->lock_observer = [this_w] (bool invalid, bool vulnerable) {
		if (auto this_l = this_w.lock ())
		{
			this_l->application.postEvent (&this_l->processor, new eventloop_event ([this_w, invalid, vulnerable] () {
				if (auto this_l = this_w.lock ())
				{
					this_l->settings.update_locked (invalid, vulnerable);
				}
			}));
		}
	};
	settings_button->setToolTip ("Unlock wallet, set password, change representative");
}

void nano_qt::wallet::refresh ()
{
	{
		auto transaction (wallet_m->wallets.tx_begin_read ());
		debug_assert (wallet_m->store.exists (transaction, account));
	}
	self.account_text->setText (QString (account.to_account ().c_str ()));
	needs_balance_refresh = true;
	accounts.refresh ();
	history.refresh ();
	account_viewer.history.refresh ();
	settings.refresh_representative ();
}

void nano_qt::wallet::update_connected ()
{
	if (node.network.empty ())
	{
		active_status.insert (nano_qt::status_types::disconnected);
	}
	else
	{
		active_status.erase (nano_qt::status_types::disconnected);
	}
}

void nano_qt::wallet::empty_password ()
{
	this->node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (3), [this] () {
		auto transaction (wallet_m->wallets.tx_begin_write ());
		wallet_m->enter_password (transaction, std::string (""));
	});
}

void nano_qt::wallet::change_rendering_ratio (nano::uint128_t const & rendering_ratio_a)
{
	application.postEvent (&processor, new eventloop_event ([this, rendering_ratio_a] () {
		this->rendering_ratio = rendering_ratio_a;
		auto balance_l (this->node.balance_pending (account, false));
		this->self.set_balance_text (balance_l);
		this->refresh ();
	}));
}

std::string nano_qt::wallet::format_balance (nano::uint128_t const & balance) const
{
	auto balance_str = nano::amount (balance).format_balance (rendering_ratio, 3, false);
	auto unit = std::string ("NANO");
	if (rendering_ratio == nano::kxrb_ratio)
	{
		unit = std::string ("knano");
	}
	else if (rendering_ratio == nano::xrb_ratio)
	{
		unit = std::string ("nano");
	}
	else if (rendering_ratio == nano::raw_ratio)
	{
		unit = std::string ("raw");
	}
	return balance_str + " " + unit;
}

void nano_qt::wallet::push_main_stack (QWidget * widget_a)
{
	main_stack->addWidget (widget_a);
	main_stack->setCurrentIndex (main_stack->count () - 1);
}

void nano_qt::wallet::pop_main_stack ()
{
	main_stack->removeWidget (main_stack->currentWidget ());
}

nano_qt::settings::settings (nano_qt::wallet & wallet_a) :
	window (new QWidget),
	layout (new QVBoxLayout),
	password (new QLineEdit),
	lock_toggle (new QPushButton ("Unlock")),
	sep1 (new QFrame),
	new_password (new QLineEdit),
	retype_password (new QLineEdit),
	change (new QPushButton ("Set/Change password")),
	sep2 (new QFrame),
	representative (new QLabel ("Account representative:")),
	current_representative (new QLabel),
	new_representative (new QLineEdit),
	change_rep (new QPushButton ("Change representative")),
	back (new QPushButton ("Back")),
	wallet (wallet_a)
{
	password->setPlaceholderText ("Password");
	password->setEchoMode (QLineEdit::EchoMode::Password);
	layout->addWidget (password);
	layout->addWidget (lock_toggle);
	sep1->setFrameShape (QFrame::HLine);
	sep1->setFrameShadow (QFrame::Sunken);
	layout->addWidget (sep1);
	new_password->setEchoMode (QLineEdit::EchoMode::Password);
	new_password->setPlaceholderText ("New password");
	layout->addWidget (new_password);
	retype_password->setEchoMode (QLineEdit::EchoMode::Password);
	retype_password->setPlaceholderText ("Retype password");
	layout->addWidget (retype_password);
	layout->addWidget (change);
	sep2->setFrameShape (QFrame::HLine);
	sep2->setFrameShadow (QFrame::Sunken);
	layout->addWidget (sep2);
	layout->addWidget (representative);
	current_representative->setTextInteractionFlags (Qt::TextSelectableByMouse);
	layout->addWidget (current_representative);
	new_representative->setPlaceholderText (wallet.node.network_params.ledger.zero_key.pub.to_account ().c_str ());
	layout->addWidget (new_representative);
	layout->addWidget (change_rep);
	layout->addStretch ();
	layout->addWidget (back);
	window->setLayout (layout);
	QObject::connect (change, &QPushButton::released, [this] () {
		auto transaction (this->wallet.wallet_m->wallets.tx_begin_write ());
		if (this->wallet.wallet_m->store.valid_password (transaction))
		{
			if (new_password->text ().isEmpty ())
			{
				new_password->clear ();
				new_password->setPlaceholderText ("Empty Password - try again: New password");
				retype_password->clear ();
				retype_password->setPlaceholderText ("Empty Password - try again: Retype password");
			}
			else
			{
				if (new_password->text () == retype_password->text ())
				{
					this->wallet.wallet_m->store.rekey (transaction, std::string (new_password->text ().toLocal8Bit ()));
					new_password->clear ();
					retype_password->clear ();
					retype_password->setPlaceholderText ("Retype password");
					show_button_success (*change);
					change->setText ("Password was changed");
					this->wallet.node.logger.try_log ("Wallet password changed");
					update_locked (false, false);
					this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
						this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
							show_button_ok (*change);
							change->setText ("Set/Change password");
						}));
					});
				}
				else
				{
					retype_password->clear ();
					retype_password->setPlaceholderText ("Password mismatch");
				}
			}
		}
		else
		{
			show_button_error (*change);
			change->setText ("Wallet is locked, unlock it");
			this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
				this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
					show_button_ok (*change);
					change->setText ("Set/Change password");
				}));
			});
		}
	});
	QObject::connect (change_rep, &QPushButton::released, [this] () {
		nano::account representative_l;
		if (!representative_l.decode_account (new_representative->text ().toStdString ()))
		{
			auto transaction (this->wallet.wallet_m->wallets.tx_begin_read ());
			if (this->wallet.wallet_m->store.valid_password (transaction))
			{
				change_rep->setEnabled (false);
				{
					auto transaction_l (this->wallet.wallet_m->wallets.tx_begin_write ());
					this->wallet.wallet_m->store.representative_set (transaction_l, representative_l);
				}
				this->wallet.wallet_m->change_sync (this->wallet.account, representative_l);
				change_rep->setEnabled (true);
				show_button_success (*change_rep);
				change_rep->setText ("Representative was changed");
				current_representative->setText (QString (representative_l.to_account ().c_str ()));
				new_representative->clear ();
				this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
					this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
						show_button_ok (*change_rep);
						change_rep->setText ("Change representative");
					}));
				});
			}
			else
			{
				show_button_error (*change_rep);
				change_rep->setText ("Wallet is locked, unlock it");
				this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
					this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
						show_button_ok (*change_rep);
						change_rep->setText ("Change representative");
					}));
				});
			}
		}
		else
		{
			show_line_error (*new_representative);
			show_button_error (*change_rep);
			change_rep->setText ("Invalid account");
			this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
				this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
					show_line_ok (*new_representative);
					show_button_ok (*change_rep);
					change_rep->setText ("Change representative");
				}));
			});
		}
	});
	QObject::connect (back, &QPushButton::released, [this] () {
		debug_assert (this->wallet.main_stack->currentWidget () == window);
		this->wallet.pop_main_stack ();
	});
	QObject::connect (lock_toggle, &QPushButton::released, [this] () {
		auto transaction (this->wallet.wallet_m->wallets.tx_begin_write ());
		if (this->wallet.wallet_m->store.valid_password (transaction))
		{
			// lock wallet
			nano::raw_key empty;
			empty.clear ();
			this->wallet.wallet_m->store.password.value_set (empty);
			update_locked (true, true);
			lock_toggle->setText ("Unlock");
			this->wallet.node.logger.try_log ("Wallet locked");
			password->setEnabled (1);
		}
		else
		{
			// try to unlock wallet
			if (!this->wallet.wallet_m->enter_password (transaction, std::string (password->text ().toLocal8Bit ())))
			{
				password->clear ();
				lock_toggle->setText ("Lock");
				password->setDisabled (1);
			}
			else
			{
				show_line_error (*password);
				show_button_error (*lock_toggle);
				lock_toggle->setText ("Invalid password");
				this->wallet.node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (5), [this] () {
					this->wallet.application.postEvent (&this->wallet.processor, new eventloop_event ([this] () {
						show_line_ok (*password);
						show_button_ok (*lock_toggle);

						// if wallet is still not unlocked by now, change button text
						auto transaction (this->wallet.wallet_m->wallets.tx_begin_write ());
						if (!this->wallet.wallet_m->store.valid_password (transaction))
						{
							lock_toggle->setText ("Unlock");
						}
					}));
				});
			}
		}
	});
	QObject::connect (new_representative, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = new_representative->cursorPosition ();
		new_representative->setText (value.trimmed ());
		new_representative->setCursorPosition (pos);
	});

	// initial state for lock toggle button
	auto transaction (this->wallet.wallet_m->wallets.tx_begin_write ());
	if (this->wallet.wallet_m->store.valid_password (transaction))
	{
		lock_toggle->setText ("Lock");
		password->setDisabled (1);
	}

	representative->setToolTip ("In the infrequent case where the network needs to make a global decision,\nyour wallet software performs a balance-weighted vote to determine\nthe outcome. Since not everyone can remain online and perform this duty,\nyour wallet names a representative that can vote with, but cannot spend,\nyour balance.");
	refresh_representative ();
}

void nano_qt::settings::refresh_representative ()
{
	auto transaction (this->wallet.wallet_m->wallets.node.store.tx_begin_read ());
	nano::account_info info;
	auto error (wallet.node.store.account.get (transaction, this->wallet.account, info));
	if (!error)
	{
		current_representative->setText (QString (info.representative.to_account ().c_str ()));
	}
	else
	{
		auto wallet_transaction (this->wallet.wallet_m->wallets.tx_begin_read ());
		current_representative->setText (this->wallet.wallet_m->store.representative (wallet_transaction).to_account ().c_str ());
	}
}

void nano_qt::settings::activate ()
{
	this->wallet.push_main_stack (window);
}

void nano_qt::settings::update_locked (bool invalid, bool vulnerable)
{
	if (invalid)
	{
		this->wallet.active_status.insert (nano_qt::status_types::locked);
	}
	else
	{
		this->wallet.active_status.erase (nano_qt::status_types::locked);
	}
	if (vulnerable)
	{
		this->wallet.active_status.insert (nano_qt::status_types::vulnerable);
	}
	else
	{
		this->wallet.active_status.erase (nano_qt::status_types::vulnerable);
	}
}

nano_qt::advanced_actions::advanced_actions (nano_qt::wallet & wallet_a) :
	window (new QWidget),
	layout (new QVBoxLayout),
	show_ledger (new QPushButton ("Ledger")),
	show_peers (new QPushButton ("Peers")),
	search_for_receivables (new QPushButton ("Search for receivables")),
	bootstrap (new QPushButton ("Initiate bootstrap")),
	wallet_refresh (new QPushButton ("Refresh Wallet")),
	create_block (new QPushButton ("Create Block")),
	enter_block (new QPushButton ("Enter Block")),
	block_viewer (new QPushButton ("Block Viewer")),
	account_viewer (new QPushButton ("Account Viewer")),
	stats_viewer (new QPushButton ("Node Statistics")),
	scale_window (new QWidget),
	scale_layout (new QHBoxLayout),
	scale_label (new QLabel ("Scale:")),
	ratio_group (new QButtonGroup),
	mnano_unit (new QRadioButton ("Mnano")),
	knano_unit (new QRadioButton ("knano")),
	nano_unit (new QRadioButton ("nano")),
	raw_unit (new QRadioButton ("raw")),
	back (new QPushButton ("Back")),
	ledger_window (new QWidget),
	ledger_layout (new QVBoxLayout),
	ledger_model (new QStandardItemModel),
	ledger_view (new QTableView),
	ledger_refresh (new QPushButton ("Refresh")),
	ledger_back (new QPushButton ("Back")),
	peers_window (new QWidget),
	peers_layout (new QVBoxLayout),
	peers_model (new QStandardItemModel),
	peers_view (new QTableView),
	peer_summary_layout (new QHBoxLayout),
	bootstrap_label (new QLabel ("IPV6:port \"::ffff:192.168.0.1:7075\"")),
	peer_count_label (new QLabel ("")),
	bootstrap_line (new QLineEdit),
	peers_bootstrap (new QPushButton ("Initiate Bootstrap")),
	peers_refresh (new QPushButton ("Refresh")),
	peers_back (new QPushButton ("Back")),
	wallet (wallet_a)
{
	ratio_group->addButton (mnano_unit);
	ratio_group->addButton (knano_unit);
	ratio_group->addButton (nano_unit);
	ratio_group->addButton (raw_unit);
	ratio_group->setId (mnano_unit, 0);
	ratio_group->setId (knano_unit, 1);
	ratio_group->setId (nano_unit, 2);
	ratio_group->setId (raw_unit, 3);
	scale_layout->addWidget (scale_label);
	scale_layout->addWidget (mnano_unit);
	scale_layout->addWidget (knano_unit);
	scale_layout->addWidget (nano_unit);
	scale_layout->addWidget (raw_unit);
	scale_window->setLayout (scale_layout);

	ledger_model->setHorizontalHeaderItem (0, new QStandardItem ("Account"));
	ledger_model->setHorizontalHeaderItem (1, new QStandardItem ("Balance"));
	ledger_model->setHorizontalHeaderItem (2, new QStandardItem ("Block"));
	ledger_view->setModel (ledger_model);
	ledger_view->setEditTriggers (QAbstractItemView::NoEditTriggers);
	ledger_view->verticalHeader ()->hide ();
	ledger_view->horizontalHeader ()->setStretchLastSection (true);
	ledger_layout->addWidget (ledger_view);
	ledger_layout->addWidget (ledger_refresh);
	ledger_layout->addWidget (ledger_back);
	ledger_layout->setContentsMargins (0, 0, 0, 0);
	ledger_window->setLayout (ledger_layout);

	peers_model->setHorizontalHeaderItem (0, new QStandardItem ("IPv6 address:port"));
	peers_model->setHorizontalHeaderItem (1, new QStandardItem ("Net version"));
	peers_model->setHorizontalHeaderItem (2, new QStandardItem ("Node ID"));
	peers_view->setEditTriggers (QAbstractItemView::NoEditTriggers);
	peers_view->verticalHeader ()->hide ();
	peers_view->setModel (peers_model);
	peers_view->setColumnWidth (0, 220);
	peers_view->setSortingEnabled (true);
	peers_view->horizontalHeader ()->setStretchLastSection (true);
	peers_layout->addWidget (peers_view);
	peer_summary_layout->addWidget (bootstrap_label);
	peer_summary_layout->addStretch ();
	peer_summary_layout->addWidget (peer_count_label);
	peers_layout->addLayout (peer_summary_layout);
	peers_layout->addWidget (bootstrap_line);
	peers_layout->addWidget (peers_bootstrap);
	peers_layout->addWidget (peers_refresh);
	peers_layout->addWidget (peers_back);
	peers_layout->setContentsMargins (0, 0, 0, 0);
	peers_window->setLayout (peers_layout);

	layout->addWidget (show_ledger);
	layout->addWidget (show_peers);
	layout->addWidget (search_for_receivables);
	layout->addWidget (bootstrap);
	layout->addWidget (wallet_refresh);
	layout->addWidget (create_block);
	layout->addWidget (enter_block);
	layout->addWidget (block_viewer);
	layout->addWidget (account_viewer);
	layout->addWidget (stats_viewer);
	layout->addWidget (scale_window);
	layout->addStretch ();
	layout->addWidget (back);
	window->setLayout (layout);

	QObject::connect (mnano_unit, &QRadioButton::toggled, [this] () {
		if (mnano_unit->isChecked ())
		{
			QSettings ().setValue (saved_ratio_key, ratio_group->id (mnano_unit));
			this->wallet.change_rendering_ratio (nano::Mxrb_ratio);
		}
	});
	QObject::connect (knano_unit, &QRadioButton::toggled, [this] () {
		if (knano_unit->isChecked ())
		{
			QSettings ().setValue (saved_ratio_key, ratio_group->id (knano_unit));
			this->wallet.change_rendering_ratio (nano::kxrb_ratio);
		}
	});
	QObject::connect (nano_unit, &QRadioButton::toggled, [this] () {
		if (nano_unit->isChecked ())
		{
			QSettings ().setValue (saved_ratio_key, ratio_group->id (nano_unit));
			this->wallet.change_rendering_ratio (nano::xrb_ratio);
		}
	});
	QObject::connect (raw_unit, &QRadioButton::toggled, [this] () {
		if (raw_unit->isChecked ())
		{
			QSettings ().setValue (saved_ratio_key, ratio_group->id (raw_unit));
			this->wallet.change_rendering_ratio (nano::raw_ratio);
		}
	});
	auto selected_ratio_id (QSettings ().value (saved_ratio_key, ratio_group->id (mnano_unit)).toInt ());
	auto selected_ratio_button = ratio_group->button (selected_ratio_id);
	debug_assert (selected_ratio_button != nullptr);

	if (selected_ratio_button)
	{
		selected_ratio_button->click ();
	}
	else
	{
		mnano_unit->click ();
	}
	QObject::connect (wallet_refresh, &QPushButton::released, [this] () {
		this->wallet.accounts.refresh ();
		this->wallet.accounts.refresh_wallet_balance ();
	});
	QObject::connect (show_peers, &QPushButton::released, [this] () {
		refresh_peers ();
		this->wallet.push_main_stack (peers_window);
	});
	QObject::connect (show_ledger, &QPushButton::released, [this] () {
		this->wallet.push_main_stack (ledger_window);
	});
	QObject::connect (back, &QPushButton::released, [this] () {
		this->wallet.pop_main_stack ();
	});
	QObject::connect (peers_back, &QPushButton::released, [this] () {
		this->wallet.pop_main_stack ();
	});
	QObject::connect (peers_bootstrap, &QPushButton::released, [this] () {
		nano::endpoint endpoint;
		auto error (nano::parse_endpoint (bootstrap_line->text ().toStdString (), endpoint));
		if (!error)
		{
			show_line_ok (*bootstrap_line);
			bootstrap_line->clear ();
			this->wallet.node.bootstrap_initiator.bootstrap (endpoint);
		}
		else
		{
			show_line_error (*bootstrap_line);
		}
	});
	QObject::connect (peers_refresh, &QPushButton::released, [this] () {
		refresh_peers ();
	});
	QObject::connect (ledger_refresh, &QPushButton::released, [this] () {
		refresh_ledger ();
	});
	QObject::connect (ledger_back, &QPushButton::released, [this] () {
		this->wallet.pop_main_stack ();
	});
	QObject::connect (search_for_receivables, &QPushButton::released, [this] () {
		std::thread ([this] { this->wallet.wallet_m->search_pending (this->wallet.wallet_m->wallets.tx_begin_read ()); }).detach ();
	});
	QObject::connect (bootstrap, &QPushButton::released, [this] () {
		std::thread ([this] { this->wallet.node.bootstrap_initiator.bootstrap (); }).detach ();
	});
	QObject::connect (create_block, &QPushButton::released, [this] () {
		this->wallet.push_main_stack (this->wallet.block_creation.window);
	});
	QObject::connect (enter_block, &QPushButton::released, [this] () {
		this->wallet.push_main_stack (this->wallet.block_entry.window);
	});
	QObject::connect (block_viewer, &QPushButton::released, [this] () {
		this->wallet.push_main_stack (this->wallet.block_viewer.window);
	});
	QObject::connect (account_viewer, &QPushButton::released, [this] () {
		this->wallet.push_main_stack (this->wallet.account_viewer.window);
	});
	QObject::connect (stats_viewer, &QPushButton::released, [this] () {
		this->wallet.push_main_stack (this->wallet.stats_viewer.window);
		this->wallet.stats_viewer.refresh_stats ();
	});

	bootstrap->setToolTip ("Multi-connection bootstrap to random peers");
	search_for_receivables->setToolTip ("Search for pending blocks");
	create_block->setToolTip ("Create block in JSON format");
	enter_block->setToolTip ("Enter block in JSON format");
}

void nano_qt::advanced_actions::refresh_peers ()
{
	peers_model->removeRows (0, peers_model->rowCount ());
	auto list (wallet.node.network.list (std::numeric_limits<size_t>::max ()));
	std::sort (list.begin (), list.end (), [] (const auto & lhs, const auto & rhs) {
		return lhs->get_endpoint () < rhs->get_endpoint ();
	});
	for (auto i (list.begin ()), n (list.end ()); i != n; ++i)
	{
		std::stringstream endpoint;
		auto channel (*i);
		endpoint << channel->to_string ();
		QString qendpoint (endpoint.str ().c_str ());
		QList<QStandardItem *> items;
		items.push_back (new QStandardItem (qendpoint));
		auto version = new QStandardItem ();
		version->setData (QVariant (channel->get_network_version ()), Qt::DisplayRole);
		items.push_back (version);
		QString node_id ("");
		auto node_id_l (channel->get_node_id_optional ());
		if (node_id_l.is_initialized ())
		{
			node_id = node_id_l.get ().to_account ().c_str ();
		}
		items.push_back (new QStandardItem (node_id));
		peers_model->appendRow (items);
	}
	peer_count_label->setText (QString ("%1 peers").arg (peers_model->rowCount ()));
}

void nano_qt::advanced_actions::refresh_ledger ()
{
	ledger_model->removeRows (0, ledger_model->rowCount ());
	auto transaction (wallet.node.store.tx_begin_read ());
	for (auto i (wallet.node.ledger.store.account.begin (transaction)), j (wallet.node.ledger.store.account.end ()); i != j; ++i)
	{
		QList<QStandardItem *> items;
		items.push_back (new QStandardItem (QString (i->first.to_account ().c_str ())));
		nano::account_info const & info (i->second);
		std::string balance;
		nano::amount (info.balance.number () / wallet.rendering_ratio).encode_dec (balance);
		items.push_back (new QStandardItem (QString (balance.c_str ())));
		std::string block_hash;
		info.head.encode_hex (block_hash);
		items.push_back (new QStandardItem (QString (block_hash.c_str ())));
		ledger_model->appendRow (items);
	}
}

void nano_qt::advanced_actions::refresh_stats ()
{
	wallet.stats_viewer.refresh_stats ();
}

nano_qt::block_entry::block_entry (nano_qt::wallet & wallet_a) :
	window (new QWidget),
	layout (new QVBoxLayout),
	block (new QPlainTextEdit),
	status (new QLabel),
	process (new QPushButton ("Process")),
	back (new QPushButton ("Back")),
	wallet (wallet_a)
{
	layout->addWidget (block);
	layout->addWidget (status);
	layout->addWidget (process);
	layout->addWidget (back);
	window->setLayout (layout);
	QObject::connect (process, &QPushButton::released, [this] () {
		auto string (block->toPlainText ().toStdString ());
		try
		{
			boost::property_tree::ptree tree;
			std::stringstream istream (string);
			boost::property_tree::read_json (istream, tree);
			auto block_l (nano::deserialize_block_json (tree));
			if (block_l != nullptr)
			{
				show_label_ok (*status);
				this->status->setText ("");
				if (!nano::work_validate_entry (*block_l))
				{
					this->wallet.node.process_active (std::move (block_l));
				}
				else
				{
					show_label_error (*status);
					this->status->setText ("Invalid work");
				}
			}
			else
			{
				show_label_error (*status);
				this->status->setText ("Unable to parse block");
			}
		}
		catch (std::runtime_error const &)
		{
			show_label_error (*status);
			this->status->setText ("Unable to parse block");
		}
	});
	QObject::connect (back, &QPushButton::released, [this] () {
		this->wallet.pop_main_stack ();
	});
}

nano_qt::block_creation::block_creation (nano_qt::wallet & wallet_a) :
	window (new QWidget),
	layout (new QVBoxLayout),
	group (new QButtonGroup),
	button_layout (new QHBoxLayout),
	send (new QRadioButton ("Send")),
	receive (new QRadioButton ("Receive")),
	change (new QRadioButton ("Change")),
	open (new QRadioButton ("Open")),
	account_label (new QLabel ("Account:")),
	account (new QLineEdit),
	source_label (new QLabel ("Source:")),
	source (new QLineEdit),
	amount_label (new QLabel ("Amount:")),
	amount (new QLineEdit),
	destination_label (new QLabel ("Destination:")),
	destination (new QLineEdit),
	representative_label (new QLabel ("Representative:")),
	representative (new QLineEdit),
	block (new QPlainTextEdit),
	status (new QLabel),
	create (new QPushButton ("Create")),
	back (new QPushButton ("Back")),
	wallet (wallet_a)
{
	group->addButton (send);
	group->addButton (receive);
	group->addButton (change);
	group->addButton (open);
	group->setId (send, 0);
	group->setId (receive, 1);
	group->setId (change, 2);
	group->setId (open, 3);

	button_layout->addWidget (send);
	button_layout->addWidget (receive);
	button_layout->addWidget (open);
	button_layout->addWidget (change);

	layout->addLayout (button_layout);
	layout->addWidget (account_label);
	layout->addWidget (account);
	layout->addWidget (source_label);
	layout->addWidget (source);
	layout->addWidget (amount_label);
	layout->addWidget (amount);
	layout->addWidget (destination_label);
	layout->addWidget (destination);
	layout->addWidget (representative_label);
	layout->addWidget (representative);
	layout->addWidget (block);
	layout->addWidget (status);
	layout->addWidget (create);
	layout->addWidget (back);
	window->setLayout (layout);
	QObject::connect (send, &QRadioButton::toggled, [this] (bool on) {
		if (on)
		{
			deactivate_all ();
			activate_send ();
		}
	});
	QObject::connect (receive, &QRadioButton::toggled, [this] (bool on) {
		if (on)
		{
			deactivate_all ();
			activate_receive ();
		}
	});
	QObject::connect (open, &QRadioButton::toggled, [this] (bool on) {
		if (on)
		{
			deactivate_all ();
			activate_open ();
		}
	});
	QObject::connect (change, &QRadioButton::toggled, [this] (bool on) {
		if (on)
		{
			deactivate_all ();
			activate_change ();
		}
	});
	QObject::connect (create, &QPushButton::released, [this] () {
		switch (group->checkedId ())
		{
			case 0:
				create_send ();
				break;
			case 1:
				create_receive ();
				break;
			case 2:
				create_change ();
				break;
			case 3:
				create_open ();
				break;
			default:
				debug_assert (false);
				break;
		}
	});
	QObject::connect (back, &QPushButton::released, [this] () {
		this->wallet.pop_main_stack ();
	});
	QObject::connect (account, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = account->cursorPosition ();
		account->setText (value.trimmed ());
		account->setCursorPosition (pos);
	});
	QObject::connect (destination, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = destination->cursorPosition ();
		destination->setText (value.trimmed ());
		destination->setCursorPosition (pos);
	});
	QObject::connect (amount, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = amount->cursorPosition ();
		amount->setText (value.trimmed ());
		amount->setCursorPosition (pos);
	});
	QObject::connect (source, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = source->cursorPosition ();
		source->setText (value.trimmed ());
		source->setCursorPosition (pos);
	});
	QObject::connect (representative, &QLineEdit::textChanged, [this] (const QString & value) {
		auto pos = representative->cursorPosition ();
		representative->setText (value.trimmed ());
		representative->setCursorPosition (pos);
	});

	send->click ();
}

void nano_qt::block_creation::deactivate_all ()
{
	account_label->hide ();
	account->hide ();
	source_label->hide ();
	source->hide ();
	amount_label->hide ();
	amount->hide ();
	destination_label->hide ();
	destination->hide ();
	representative_label->hide ();
	representative->hide ();
}

void nano_qt::block_creation::activate_send ()
{
	account_label->show ();
	account->show ();
	amount_label->show ();
	amount->show ();
	destination_label->show ();
	destination->show ();
}

void nano_qt::block_creation::activate_receive ()
{
	source_label->show ();
	source->show ();
}

void nano_qt::block_creation::activate_open ()
{
	source_label->show ();
	source->show ();
	representative_label->show ();
	representative->show ();
}

void nano_qt::block_creation::activate_change ()
{
	account_label->show ();
	account->show ();
	representative_label->show ();
	representative->show ();
}

void nano_qt::block_creation::create_send ()
{
	nano::account account_l;
	auto error (account_l.decode_account (account->text ().toStdString ()));
	if (!error)
	{
		nano::amount amount_l;
		error = amount_l.decode_dec (amount->text ().toStdString ());
		if (!error)
		{
			nano::account destination_l;
			error = destination_l.decode_account (destination->text ().toStdString ());
			if (!error)
			{
				auto transaction (wallet.node.wallets.tx_begin_read ());
				auto block_transaction (wallet.node.store.tx_begin_read ());
				nano::raw_key key;
				if (!wallet.wallet_m->store.fetch (transaction, account_l, key))
				{
					auto balance (wallet.node.ledger.account_balance (block_transaction, account_l));
					if (amount_l.number () <= balance)
					{
						nano::account_info info;
						auto error (wallet.node.store.account.get (block_transaction, account_l, info));
						(void)error;
						debug_assert (!error);
						nano::state_block send (account_l, info.head, info.representative, balance - amount_l.number (), destination_l, key, account_l, 0);
						nano::block_details details;
						details.is_send = true;
						details.epoch = info.epoch ();
						auto const required_difficulty{ nano::work_threshold (send.work_version (), details) };
						if (wallet.node.work_generate_blocking (send, required_difficulty).is_initialized ())
						{
							std::string block_l;
							send.serialize_json (block_l);
							block->setPlainText (QString (block_l.c_str ()));
							show_label_ok (*status);
							status->setText ("Created block");
						}
						else
						{
							debug_assert (required_difficulty <= wallet.node.max_work_generate_difficulty (send.work_version ()));
							show_label_error (*status);
							if (wallet.node.work_generation_enabled ())
							{
								status->setText ("Work generation failure");
							}
							else
							{
								status->setText ("Work generation is disabled");
							}
						}
					}
					else
					{
						show_label_error (*status);
						status->setText ("Insufficient balance");
					}
				}
				else
				{
					show_label_error (*status);
					status->setText ("Account is not in wallet");
				}
			}
			else
			{
				show_label_error (*status);
				status->setText ("Unable to decode destination");
			}
		}
		else
		{
			show_label_error (*status);
			status->setText ("Unable to decode amount");
		}
	}
	else
	{
		show_label_error (*status);
		status->setText ("Unable to decode account");
	}
}

void nano_qt::block_creation::create_receive ()
{
	nano::block_hash source_l;
	auto error (source_l.decode_hex (source->text ().toStdString ()));
	if (!error)
	{
		auto transaction (wallet.node.wallets.tx_begin_read ());
		auto block_transaction (wallet.node.store.tx_begin_read ());
		auto block_l (wallet.node.store.block.get (block_transaction, source_l));
		if (block_l != nullptr)
		{
			auto const & destination (wallet.node.ledger.block_destination (block_transaction, *block_l));
			if (!destination.is_zero ())
			{
				nano::pending_key pending_key (destination, source_l);
				nano::pending_info pending;
				if (!wallet.node.store.pending.get (block_transaction, pending_key, pending))
				{
					nano::account_info info;
					auto error (wallet.node.store.account.get (block_transaction, pending_key.account, info));
					if (!error)
					{
						nano::raw_key key;
						auto error (wallet.wallet_m->store.fetch (transaction, pending_key.account, key));
						if (!error)
						{
							nano::state_block receive (pending_key.account, info.head, info.representative, info.balance.number () + pending.amount.number (), source_l, key, pending_key.account, 0);
							nano::block_details details;
							details.is_receive = true;
							details.epoch = std::max (info.epoch (), pending.epoch);
							auto required_difficulty{ nano::work_threshold (receive.work_version (), details) };
							if (wallet.node.work_generate_blocking (receive, required_difficulty).is_initialized ())
							{
								std::string block_l;
								receive.serialize_json (block_l);
								block->setPlainText (QString (block_l.c_str ()));
								show_label_ok (*status);
								status->setText ("Created block");
							}
							else
							{
								debug_assert (required_difficulty <= wallet.node.max_work_generate_difficulty (receive.work_version ()));
								show_label_error (*status);
								if (wallet.node.work_generation_enabled ())
								{
									status->setText ("Work generation failure");
								}
								else
								{
									status->setText ("Work generation is disabled");
								}
							}
						}
						else
						{
							show_label_error (*status);
							status->setText ("Account is not in wallet");
						}
					}
					else
					{
						show_label_error (*status);
						status->setText ("Account not yet open");
					}
				}
				else
				{
					show_label_error (*status);
					status->setText ("Source block is not pending to receive");
				}
			}
			else
			{
				show_label_error (*status);
				status->setText ("Source is not a send block");
			}
		}
		else
		{
			show_label_error (*status);
			status->setText ("Source block not found");
		}
	}
	else
	{
		show_label_error (*status);
		status->setText ("Unable to decode source");
	}
}

void nano_qt::block_creation::create_change ()
{
	nano::account account_l;
	auto error (account_l.decode_account (account->text ().toStdString ()));
	if (!error)
	{
		nano::account representative_l;
		error = representative_l.decode_account (representative->text ().toStdString ());
		if (!error)
		{
			auto transaction (wallet.node.wallets.tx_begin_read ());
			auto block_transaction (wallet.node.store.tx_begin_read ());
			nano::account_info info;
			auto error (wallet.node.store.account.get (block_transaction, account_l, info));
			if (!error)
			{
				nano::raw_key key;
				auto error (wallet.wallet_m->store.fetch (transaction, account_l, key));
				if (!error)
				{
					nano::state_block change (account_l, info.head, representative_l, info.balance, 0, key, account_l, 0);
					nano::block_details details;
					details.epoch = info.epoch ();
					auto const required_difficulty{ nano::work_threshold (change.work_version (), details) };
					if (wallet.node.work_generate_blocking (change, required_difficulty).is_initialized ())
					{
						std::string block_l;
						change.serialize_json (block_l);
						block->setPlainText (QString (block_l.c_str ()));
						show_label_ok (*status);
						status->setText ("Created block");
					}
					else
					{
						debug_assert (required_difficulty <= wallet.node.max_work_generate_difficulty (change.work_version ()));
						show_label_error (*status);
						if (wallet.node.work_generation_enabled ())
						{
							status->setText ("Work generation failure");
						}
						else
						{
							status->setText ("Work generation is disabled");
						}
					}
				}
				else
				{
					show_label_error (*status);
					status->setText ("Account is not in wallet");
				}
			}
			else
			{
				show_label_error (*status);
				status->setText ("Account not yet open");
			}
		}
		else
		{
			show_label_error (*status);
			status->setText ("Unable to decode representative");
		}
	}
	else
	{
		show_label_error (*status);
		status->setText ("Unable to decode account");
	}
}

void nano_qt::block_creation::create_open ()
{
	nano::block_hash source_l;
	auto error (source_l.decode_hex (source->text ().toStdString ()));
	if (!error)
	{
		nano::account representative_l;
		error = representative_l.decode_account (representative->text ().toStdString ());
		if (!error)
		{
			auto transaction (wallet.node.wallets.tx_begin_read ());
			auto block_transaction (wallet.node.store.tx_begin_read ());
			auto block_l (wallet.node.store.block.get (block_transaction, source_l));
			if (block_l != nullptr)
			{
				auto const & destination (wallet.node.ledger.block_destination (block_transaction, *block_l));
				if (!destination.is_zero ())
				{
					nano::pending_key pending_key (destination, source_l);
					nano::pending_info pending;
					if (!wallet.node.store.pending.get (block_transaction, pending_key, pending))
					{
						nano::account_info info;
						auto error (wallet.node.store.account.get (block_transaction, pending_key.account, info));
						if (error)
						{
							nano::raw_key key;
							auto error (wallet.wallet_m->store.fetch (transaction, pending_key.account, key));
							if (!error)
							{
								nano::state_block open (pending_key.account, 0, representative_l, pending.amount, source_l, key, pending_key.account, 0);
								nano::block_details details;
								details.is_receive = true;
								details.epoch = pending.epoch;
								auto const required_difficulty{ nano::work_threshold (open.work_version (), details) };
								if (wallet.node.work_generate_blocking (open, required_difficulty).is_initialized ())
								{
									std::string block_l;
									open.serialize_json (block_l);
									block->setPlainText (QString (block_l.c_str ()));
									show_label_ok (*status);
									status->setText ("Created block");
								}
								else
								{
									debug_assert (required_difficulty <= wallet.node.max_work_generate_difficulty (open.work_version ()));
									show_label_error (*status);
									if (wallet.node.work_generation_enabled ())
									{
										status->setText ("Work generation failure");
									}
									else
									{
										status->setText ("Work generation is disabled");
									}
								}
							}
							else
							{
								show_label_error (*status);
								status->setText ("Account is not in wallet");
							}
						}
						else
						{
							show_label_error (*status);
							status->setText ("Account already open");
						}
					}
					else
					{
						show_label_error (*status);
						status->setText ("Source block is not pending to receive");
					}
				}
				else
				{
					show_label_error (*status);
					status->setText ("Source is not a send block");
				}
			}
			else
			{
				show_label_error (*status);
				status->setText ("Source block not found");
			}
		}
		else
		{
			show_label_error (*status);
			status->setText ("Unable to decode representative");
		}
	}
	else
	{
		show_label_error (*status);
		status->setText ("Unable to decode source");
	}
}
