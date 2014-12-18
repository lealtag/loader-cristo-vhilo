import datetime 
tasks_schedule ={   'beginning_window': 1,
    'main_worker': [1, 1, 0],
    'rebuild_worker': [1, 6, 0],
    'recovery_worker': [1, 1, 0],
    'stopper': 0,
    'subprocess_worker': [0, 0, 0],
    'time_interval': 60,
    'tolerance': 60,
    'updater_worker': [20, 6, 0]}
kill_on_update ={'main_worker': 0, 'rebuild_worker': 1, 'recovery_worker': 0}
