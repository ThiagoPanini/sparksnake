# Sparksnake: Taking Spark to the Next Level

## Overview

The *sparksnake* is a Python library that provides a set of tools to enhance the development of Spark applications. From beginners to experienced users, everyone who uses Spark in a local environment or even in a cloud provider can enjoy *sparksnake's* features and take the development of their applications to the next level!

- Have you ever wanted to use pre built functions for common Spark operations?
- Have you ever wanted to deep dive into AWS services like Glue and EMR but got stuck on how to set up the service?
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

- 🤖 Apply common Spark operations using few lines of code
- 💻 Start developing your Spark applications anywhere using the "default" mode or in any AWS services that uses Spark
- ⏳ Stop spending time setting up the boring stuff of your Spark applications
- 💡 Apply the best practices on your application by structuring your code following the best practices
- 👁️‍🗨️ Improve your aplication's observability by using detailed log messages on CloudWatch and exception handlers


## How Does it Work?

Whenever users need to simplify the development of their Spark applications, the *sparksnake* Python library can be used. It is available on [PyPI](https://pypi.org/project/sparksnake/) and can be installed using the `pip install sparksnake` command. One of the main goals of *sparksnake* is to put together common Spark operations as class methods that follows the best practices and can be used in any pyspark application.

The *sparksnake* library also helps users that are deploying Spark applications on AWS services like Glue and EMR by providing an elegant and easy way to execute specific Spark operations used on those services. For example, you can initialize a Glue job, get all job arguments and get all session and context elements using a single `init_job()` method.

But don't worry! Everything you need to know about *sparksnake* is on this documentation page. Enjoy the read and don't miss the oportunity to start using this huge solution.

## Combining Solutions

The *sparksnake* Python package isn't alone. There are other complementary open source solutions that can be put together to enable the full power of learning analytics on AWS. [Check it out](https://github.com/ThiagoPanini) if you think they could be useful for you!

![A diagram showing how its possible to use other solutions like datadelivery, terraglue and sparksnake](https://github.com/ThiagoPanini/datadelivery/blob/main/docs/assets/imgs/products-overview-v2.png?raw=true)

## Read the Docs

- How about the [story](story.md) about the library creation? I think you will like it!
- Check the [Quickstart](./quickstart/basic-tutorial.md) section to start using *sparsnake*
- In the [Features](./features/demo-glue.md) page you will find usage demos to help you to extract the full power of *sparksnake*
- The [Official Docs](./mkdocstrings/gluejobmanager.md) page brings all modules, classes and methods documentation in details

## Contact Me

- :fontawesome-brands-github: [@ThiagoPanini](https://github.com/ThiagoPanini)
- :fontawesome-brands-linkedin: [Thiago Panini](https://www.linkedin.com/in/thiago-panini/)
- :fontawesome-brands-hashnode: [panini-tech-lab](https://panini.hashnode.dev/)
