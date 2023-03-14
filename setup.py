"""
Arquivo de configuração da biblioteca.

Aqui são consolidadas todas as informações refentes ao pacote
publicado e disponibilizado no PyPI
"""
# Importando bibliotecas
from setuptools import setup, find_packages

# Lendo README.md
with open("README.md", "r", encoding='utf-8') as f:
    __long_description__ = f.read()

# Criando setup
setup(
    name='sparksnake',
    version='0.1.4',
    author='Thiago Panini',
    author_email='panini.development@gmail.com',
    packages=find_packages(),
    install_requires=[
        "pyspark"
    ],
    license='MIT',
    description="Funcionalidades Spark criadas para facilitar a criação de "
                "aplicações que utilizam este framework em serviços AWS",
    long_description=__long_description__,
    long_description_content_type="text/markdown",
    url='https://github.com/ThiagoPanini/sparksnake',
    keywords='Cloud, AWS, Python, Spark, pyspark',
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Natural Language :: Portuguese (Brazilian)",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    python_requires=">=3.0.0"
)
