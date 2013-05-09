Cumulus
=======

Helps manage AWS CloudFormation stacks

The problem
----------------------------------

Amazon CloudFormation (CF) allows you to instantiate multiple AWS resources in a repeatable, ordered and structured method. As our infrastructure grew, so did our CF templates and soon they were monolothic and complex to maintain. We looked at spliting these templates into smaller chunks, which worked as a short term solution but created a new problem. With multiple templates dependant on other declared resources, we were forced to manually pass parameters for inter-stack operability. This greatly affected the repeatability of our stacks as we did not have an easy method to keep track of what parameters were used, especially those relating to physical resource IDs.

The solution
------------

Cumulus attempts to solve the problem by introducing a layer above CF templates, a stack configuration YAML file. This allows multiple CF stacks to be created in order and maintained respecting their dependencies. The YAML file stores values for parameters to be passed into each of the stacks. Parameters can be assigned with static values or will source the value of a parameter, output or resource of another stack described in the YAML file.  Cumulus actively translates reference values to physical resource values on creation of the stack.

Current state / known issues
----------------------------

For our use, Cumulus can create, update and delete stacks reliably but is still very much in an Alpha state. We're looking forward to see how you use Cumulus, and please submit pull requests for any issues you may encounter or for feature requests :)

This is my first real python project, so I'm sure the code can be, just generally better...

Known issues:

* Templates are passed in as a JSON string to CF, this will break large templates
* Cumulus generated stacks display as one liners in the CF console

Roadmap:

* Implement a way of displaying meaningful diffs during update runs
* Add support for using S3/Externally hosted templates
* Support larger templates

How to get started
----------------

Clone the repo somewhere:

        $ git clone git://github.com/cotdsa/cumulus.git

Install Cumulus with setuptools:

        $ sudo python setup.py install

Make sure you have AWS credentials set up for boto (the library used by Cumulus to interact with AWS). Set the following environment variables:

**AWS_ACCESS_KEY_ID** - Your AWS Access Key ID

**AWS_SECRET_ACCESS_KEY** - Your AWS Secret Access Key

or create a boto config file as described [here](http://code.google.com/p/boto/wiki/BotoConfig), covering some other helpful boto-related settings.

Creating the example stack
--------------------------

**Common sense warning:** Running this example will create real resources in AWS and will cost you AWS credits / money / magic beans.

I have included an example stack in the examples/ dir. It consists of three files:

* cumulus\_example\_stack.yaml: The Cumulus yaml file for the stack. Creates a stack out of the following two templates in ap-southeast-2 (Sydney region)
* vpc\_layer.json: CF template to creates a VPC, base subnet and ACL
* instance\_layer.json: CF template to create an instance inside a given VPC

The template files are complete and work independently of Cumulus. Cumulus's purpose in life is just to make managing them easier.

To create the example stack, change into the examples/ dir and run:

        $ cumulus -y cumulus_example_stack.yaml -a create

Cumulus will print out CF messages as it builds.

You can then try modifying the template and/or the values of the parameters and then update the stack:

        $ cumulus -y cumulus_example_stack.yaml -a update

Once you have finished experimenting, you can delete as follows:

        $ cumulus -y cumulus_example_stack.yaml -a delete

General usage
-------------

        cumulus -h
        usage: cumulus [-h] -y YAMLFILE -a ACTION [-l LOGLEVEL] [-L BOTOLOGLEVEL]
                       [-s STACKNAME]

        optional arguments:
          -h, --help            show this help message and exit
          -y YAMLFILE, --yamlfile YAMLFILE
                                The yaml file to read the VPC mega stack configuration
                                from
          -a ACTION, --action ACTION
                                The action to preform: create, check, update, delete
                                or watch
          -l LOGLEVEL, --log LOGLEVEL
                                Log Level for output messages, CRITICAL, ERROR,
                                WARNING, INFO or DEBUG
          -L BOTOLOGLEVEL, --botolog BOTOLOGLEVEL
                                Log Level for boto, CRITICAL, ERROR, WARNING, INFO or
                                DEBUG
          -s STACKNAME, --stack STACKNAME
                                The stack name, used with the watch action, ignored
                                for other actions

YAML file format
----------------

Have a look at examples/cumulus\_example\_stack.yaml for a commented version of the yaml file.

All sections are required at the moment, even if they are blank (i.e. depends, params). depends also needs to be empty or an array, even if the stack has only one dependency.
