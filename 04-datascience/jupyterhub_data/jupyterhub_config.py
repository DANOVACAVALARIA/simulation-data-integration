from dockerspawner import DockerSpawner
from firstuseauthenticator import FirstUseAuthenticator

import os
import logging

c = get_config()

# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# volume_path = os.path.join(BASE_DIR, '..', '..', 'shared-volume')

c.JupyterHub.bind_url = 'http://0.0.0.0:8000'
c.JupyterHub.hub_connect_ip = 'jupyterhub-app'

# diretório de dados do jupyterHub
# c.JupyterHub.data_files_path = '/data'

# configurando o authenticator
c.JupyterHub.authenticator_class = FirstUseAuthenticator

# o FirstUseAuthenticator cria a senha no primeiro login
c.FirstUseAuthenticator.create_users = True

# configurando o spawner
c.JupyterHub.spawner_class = DockerSpawner

# configurando o DockerSpawner
c.DockerSpawner.image = 'quay.io/jupyter/base-notebook:latest'

# diretórios de trabalho e montagem de volumes
notebook_dir = '/home/jovyan/work'
c.DockerSpawner.notebook_dir = notebook_dir
c.DockerSpawner.volumes = {
    'jupyterhub-user-{username}': notebook_dir,
    # volume_path: '/shared-volume'
    # 'D:/05 - Projetos Web-DS/04 - mestrado (arquitetura)/03-http-auth-v3/shared-volume': '/shared-volume'
    './shared_volume': '/shared-volume'
}

# recursos adicionais de inicialização
#c.DockerSpawner.extra_create_kwargs = {'user': 'root'}  # Permitir configurações adicionais na inicialização
c.DockerSpawner.remove_containers = True  # Remover contêineres quando os usuários fizerem logout
c.DockerSpawner.debug = True

# configurações de rede do DockerSpawner
c.DockerSpawner.use_internal_ip = True
# c.DockerSpawner.network_name = 'jupyterhub-net'
# c.DockerSpawner.network_name = '03-http-auth-v3_ebsim-net'
c.DockerSpawner.network_name = 'simulation-data-integration_ebsim-net'

# configurações de administração
c.JupyterHub.admin_access = True
c.Authenticator.admin_users = {'admin'}

# persistência de dados de usuários
c.JupyterHub.db_url = 'sqlite:////jupyterhub.sqlite'

# configurações de segurança
c.JupyterHub.cookie_secret_file = './jupyterhub_cookie_secret'

# serviço do proxy
# c.ConfigurableHTTPProxy.api_url = 'http://0.0.0.0:8005'