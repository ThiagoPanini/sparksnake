# Getting Started with sparksnake


## Installing from pip

The latest version of *sparksnake* library is already published in [PyPI](https://pypi.org/project/sparksnake/) and available free of charge for anyone interested in improving the creation of their Spark applications using AWS services such as Glue and EMR. To start your journey, simply perform your installation using the following command:

```bash
# Installing sparksnake library
pip install sparksnake
```

??? tip "About Python virtual environments"
    In general, it's a good practice to create a [virtual environment](https://docs.python.org/3/library/venv.html) before starting a Python project. This approach allows you to have an isolated environment with more refined control over dependencies.
    
    ??? example "Creating virtual environments"
        Creating Python virtual environments is easy. You can open your terminal or command prompt and run the following command in any directory of your preference.

        ```bash
        python -m venv <venv_name>
        ```

        Where `<venv_name>` should be replaced by the name of your brand new virtual environment. As an additional tip, you can have virtual environment names associated with project names (e.g. `project_venv`) for allowing you to remember the goal of each venv.


    ??? example "Accessing virtual environments"
        Creating a virtual env is just the first step of the process. After that, it's important to explicity access it to ensure that every action you perform (e.g. installing a new Python package) will be performed inside the virtual env.

        If you use Windows as your OS, then use the command below to access the Python virtual environment:

        ```bash
        # Accessing venvs on Windows
        <venv_path>/Scripts/activate
        ```

        In case you are using a Linux machine (or Git Bash in Windows), the command has minor changes and is given by:

        ```bash
        # Accessing venvs on Linux
        source <venv_path>/Scripts/activate
        ```

        Where `<venv_path>` is the location reference of the newly created virtual environment. For example, if you created a virtual environment named *test_venv* in your user directory, then `<venv_path>` can be replaced by `C:\Users\username\test_venv` on Windows or simply `~/test_venv` on Linux.
        
    
    For more information, this [excellent Real Python blog article](https://realpython.com/python-virtual-environments-a-primer/) may shed light on a number of questions involving the creation and use of Python virtual environments.


## Start Building your Spark Application

Well, once you have *sparksnake* installed in your environment, you can use it wherever you need do create a Spark application script. The first task is to **choose an operation mode**.

???+ question "What? What is an operation mode in sparksnake? Which modes are available?"
    No worries at this time, Spark developer! I prepared a really detailed demo section explaining all this for you.
    
    :material-alert-decagram:{ .mdx-pulse .warning }  Check the [Library Structure](../demos/lib-structure.md) page to start getting your hands dirty with *sparksnake*. I must advise you that this is a one-way street and you probably won't never want to develop Spark applications as you did until now.