# Sparksnake: Taking Spark to the Next Level

## Overview

The *sparksnake* library provides a set of tools to enhance the development of Spark applications, specially if they are meant to be deployed and used in AWS services, such as Glue and EMR.

- Have you ever wanted to improve your Spark code with log messages and exception handling?
- Have you ever wanted to go in a deep dive in AWS services with Spark but you got stuck setting up your code?
- Have you ever wanted to optimize your 1k+ lines of Spark code deployed on AWS?

✨ Try *sparksnake*!


<div align="center">
    <br><img src="https://github.com/ThiagoPanini/sparksnake/blob/main/docs/assets/imgs/header-readme.png?raw=true" alt="sparksnake-logo">
</div>


<div align="center">  
  <br>
  <a href="https://pypi.org/project/sparksnake/">
    <img src="https://img.shields.io/pypi/v/sparksnake?color=purple" alt="Shield sparksnake PyPI version">
  </a>

  <a href="https://pypi.org/project/sparksnake/">
    <img src="https://img.shields.io/pypi/dm/sparksnake?color=purple" alt="Shield sparksnake PyPI downloads">
  </a>

  <a href="https://pypi.org/project/sparksnake/">
    <img src="https://img.shields.io/pypi/status/sparksnake?color=purple" alt="Shield sparksnake PyPI status">
  </a>
  
  <img src="https://img.shields.io/github/last-commit/ThiagoPanini/sparksnake?color=purple" alt="Shield github last commit">

  <br>
  
  <img src="https://img.shields.io/github/actions/workflow/status/ThiagoPanini/sparksnake/ci-main.yml?label=ci" alt="Shield github CI workflow">

  <a href='https://sparksnake.readthedocs.io/pt/latest/?badge=latest'>
    <img src='https://readthedocs.org/projects/sparksnake/badge/?version=latest' alt='Documentation Status' />
  </a>

  <a href="https://codecov.io/gh/ThiagoPanini/sparksnake" > 
    <img src="https://codecov.io/gh/ThiagoPanini/sparksnake/branch/main/graph/badge.svg?token=zSdFO9jkD8"/> 
  </a>

</div>

___


## Features

- 🤖 Apply common Spark operations using a few lines of code
- 💻 Start developing your Spark applications locally using the "local" mode or in any AWS services that uses Spark
- 💡 Use methods available to turn your life as easy as possible
- ⏳ You don't need to spend time setting up your Spark applications (locally or in a Glue job, for example)
- 👁️‍🗨️ Enhanced observability by providing detailed log messages on CloudWatch and exception handlers


## How Does it Work?

Whenever users need to simplify the development of their Spark applications, the **sparksnake** Python library can be used. It is available on [PyPI]() and can be installed using the `pip install sparksnake` command. So, in any Python file, users can import the package and start the creation of their application by starting the `SparkManager` class using a chosen mode based on where this Spark application will be deployed.

???+ question "What do you mean by a 'chosen mode'?"
    Well, the idea is quite simple: the **sparksnake** library was thought to help users to build their Spark applications regardless on where it will be deployed. In other words, if you are running Spark applications locally or in AWS services like Glue and EMR, sparksnake can be fit perfect for you.

    Based on this strategy, the library can be set with a `mode` attribute that handles class inheritance and enables all specific features for users according to the environment where Spark is running. You will certainly find more information about this huge feature along this documentation.

## Combining Solutions

The *sparksnake* Python package isn't alone. There are other complementary open source solutions that can be put together to enable the full power of learning analytics on AWS. [Check it out](https://github.com/ThiagoPanini) if you think they could be useful for you!

![A diagram showing how its possible to use other solutions like datadelivery, terraglue and sparksnake](https://github.com/ThiagoPanini/datadelivery/blob/main/docs/assets/imgs/products-overview-v2.png?raw=true)

## Read the Docs

- How about the [story](story.md) about the library creation? I think you will like it!
- Check the [Quickstart](./quickstart/basic-tutorial.md) section to start using *sparsnake*
- At [Features](./features/demo-glue.md) page you will find usage demos to help you extract the full power of *sparksnake*
- The [Official Docs](./mkdocstrings/gluejobmanager.md) page brings all modules, classes and methods documentation in details

## Contacts

- :fontawesome-brands-github: [@ThiagoPanini](https://github.com/ThiagoPanini)
- :fontawesome-brands-linkedin: [Thiago Panini](https://www.linkedin.com/in/thiago-panini/)
- :fontawesome-brands-hashnode: [panini-tech-lab](https://panini.hashnode.dev/)
