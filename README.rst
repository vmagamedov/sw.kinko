Star Wars Playground
====================

This is a sample application to try Kinko DSL and Hiku Graph API.

Installation
~~~~~~~~~~~~

Setup environment::

  $ git clone https://github.com/vmagamedov/sw.kinko.git && cd sw.kinko
  $ pyvenv env
  $ source env/bin/activate
  $ pip install -r requirements.txt
  
Then run example::
  
  $ python sw.wsgi
   * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)

To view rendered
`swapp/ui/index.kinko <https://github.com/vmagamedov/sw.kinko/blob/master/swapp/ui/index.kinko>`_
template, you can open http://localhost:5000/ url. In the console you will see generated Hiku query
and result of it's execution.

To view Graph API Console, you can open http://localhost:5000/graph url. Try this query:

.. code-block:: clojure

  [{:features [:title :director {:planets [:name]}]}]
