c = get_config()
c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = 8899
c.ServerApp.token = 'jupyter'
c.ServerApp.password = ''
c.ServerApp.terminado_settings = {'shell_command': ['/usr/bin/zsh']}
