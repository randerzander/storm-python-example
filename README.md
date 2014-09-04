#Dependencies:
yum groupinstall -y 'Development Tools'
yum install -y python-devel

#Download and install virtualenv
wget https://bootstrap.pypa.io/ez_setup.py
sudo python ez_setup.py
sudo easy_install pip
sudo pip install virtualenv

#Create a relocatable Python virtualenv
virtualenv pyenv
