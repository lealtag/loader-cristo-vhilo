routes = {
	'clients': '/api/clients.json',
	'errors': '/api/logs.json',
	'invoices': '/api/invoices.json',
	'cancelinvoices': '/api/cancelinvoice.json',
	'log_error_file': 'errors.log',
	'login': '/api/users/login.json',
	'test': '/api/logs?quantity=1',
	'owners': '/api/owners.json',
	'port': '80',
	'products': '/api/products.json',
	'reclients': '/api/reclients.json',
	'reproducts': '/api/reproducts.json',
	'reinvoices' : '/api/reinvoices.json',
	'recancelinvoices': '/api/recancelinvoice.json',
	'url': 'http://xdata.lealtag.com',
	'fileURL':'weeknd.cloudapp.net',
	'filePort':22
}


rebuild_params ={   'log_error_file': 'pkg\\log\\errors_rebuilder.log',
    'log_file': 'pkg\\log\\rebuilder.log',
    'log_size': 2
}


updater_params ={   'log_error_file': 'pkg\\log\\errors_updater.log',
    'log_file': 'pkg\\log\\updater.log',
    'log_size': 2
}


recovery_params ={   'log_error_file': 'pkg\\log\\errors_rl.log',
    'log_file': 'pkg\\log\\rl.log',
    'log_size': 2
}


master_params ={
	'log_error_file': 'pkg\\log\\errors_master.log',
	'log_file': 'pkg\\log\\master.log',
	'log_size': 2
}

main_params ={   
	'log_error_file': 'pkg\\log\\errors_ml.log',
    'log_file': 'pkg\\log\\ml.log',
    'log_size': 2
}
