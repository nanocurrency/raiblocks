#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/config.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/common.hpp>
#include <nano/node/testing.hpp>
#include <nano/qt/qt.hpp>

#include <boost/format.hpp>

#include <thread>
int main (int argc, char ** argv)
{
	nano::force_nano_dev_network ();
	nano::node_singleton_memory_pool_purge_guard memory_pool_cleanup_guard;
	QApplication application (argc, argv);
	QCoreApplication::setOrganizationName ("Nano");
	QCoreApplication::setOrganizationDomain ("nano.org");
	QCoreApplication::setApplicationName ("Nano Wallet");
	nano_qt::eventloop_processor processor;
	const uint16_t count (16);
	nano::system system (count);
	nano::thread_runner runner (system.env.ctx, system.nodes[0]->config.io_threads);
	std::unique_ptr<QTabWidget> client_tabs (new QTabWidget);
	std::vector<std::unique_ptr<nano_qt::wallet>> guis;
	for (auto i (0); i < count; ++i)
	{
		auto wallet (system.nodes[i]->wallets.create (nano::random_wallet_id ()));
		nano::keypair key;
		wallet->insert_adhoc (key.prv);
		guis.push_back (std::make_unique<nano_qt::wallet> (application, processor, *system.nodes[i], wallet, key.pub));
		client_tabs->addTab (guis.back ()->client_window, boost::str (boost::format ("Wallet %1%") % i).c_str ());
	}
	client_tabs->show ();
	QObject::connect (&application, &QApplication::aboutToQuit, [&]() {
		system.stop ();
	});
	int result;
	try
	{
		result = application.exec ();
	}
	catch (...)
	{
		result = -1;
		debug_assert (false);
	}
	runner.join ();
	return result;
}
