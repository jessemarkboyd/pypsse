from distutils.core import setup
setup(
    name = 'pypsse',
    author = 'Jesse Boyd',
    author_email = 'jessemarkboyd@gmail.com',
    url = 'https://github.com/jessemarkboyd/pypsse',
    download_url = 'https://github.com/jessemarkboyd/pypsse/tarball/0.1.2',
    keywords = ['testing', 'logging', 'psse', 'powerflow', 'loadflow'],
    version = '0.1.2',
    description = 'PSSE API wrapper for Python',
    long_description = open('README.txt').read(),
    py_modules = ['pypsse'],
    license = 'Creative Commons Attribution-Noncommercial-Share Alike license',
    install_requires = ['pandas','numpy','psse34','Tkinter','os','sys','re','StringIO'],
)
