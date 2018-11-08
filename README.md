# xcute

**WARNING**: this project is deprecated. Please use the new version at https://github.com/viert/xc

xcute is a convenient parallel execution tool backed up by https://github.com/viert/conductor as a hosts store and classifier.
It uses ssh for servers' access and can execute commands remotely in three different ways: serially, parallel execution with streaming output and collapsing same outputs.

## installation

`pip install git+https://github.com/viert/xcute`

## Ubuntu installation

`apt install python-termcolor python-gevent python-requests`

## Docker run
`docker build -t xcute .`

`docker run --rm -it --env CONDUCTOR_HOST="[conductor_host]" --env PROJECT_LIST="[project_list]" --env CONDUCTOR_USER="[user]" xcute`
