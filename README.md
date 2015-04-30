Chicago Coders Conference: Big Data Hands on Lab
================================================

In this exercise, we'll be duplicating research performed by investigative reporters at the Chicago Tribune as part of their award-winning series on red light cameras in Chicago. In case you're unfamiliar with this research, the Tribune series discovered (among other things) that:

*	Select red-light cameras seemed to go on ticket-issuing benders. All of a sudden, affected cameras began nabbing drivers at a rate sometimes in excess of 50x their historical average.

*	Such spikes were often preceded and succeeded by periods of no activity, suggesting perhaps that cameras were reconfigured without mandated notice or documentation.

*	Drivers that appeal red-light tickets typically win their cases 10% of the time, but tickets issued during these spikes were overturned 45% of the time. 

Specifically, we'll be reproducing the data (found in this article, http://apps.chicagotribune.com/news/local/red-light-camera-tickets/) illustrating periods of abnormally high ticketing activity and identifying any correlation to periods of appeal success. 

The full investigavtive series can be found here, http://www.chicagotribune.com/news/watchdog/redlight/. 

Getting Started
---------------

We'll be using the Hortonworks Sandbox v2.1 as our "big data" platform during this exercise. The sandbox is freely available, installs trivially, and provides a user friendly interface. IUt even has its own built-in tutorial which you can use to improve your skillset outside this course. 

Most host machines should have little trouble running the sandbox, but official requirements from Hortonworks state:

* Windows XP, Windows 7, Windows 8 or Mac OS X
* Minimum 4GB RAM; 8GB required to run Ambari and Hbase
* Virtualization enabled on BIOS
* Browser: Chrome 25+, IE 9+, Safari 6+ recommended. (Sandbox will not run on IE 10)

#### Install and run the Hortonworks virtual machine:

1. Locate the VirtualBox software on the provided thumb drive and install it on your Mac or PC.

2. Import the Hortonworks virtual machine (OVA) into VirtualBox by double-clicking the `.ova` file on the thumb drive. Note that the import process may take several minutes. 

3. Once imported, click the "Start" button to run the virtual machine. 
   
4. The Hortonworks sandbox runs as a web app; the UI front-end is a Hortonworks development called "Hue". As soon as the virtual machine has booted its console will display the URL to access the application. Open a browser and navigate to that URL: http://127.0.0.1:8888

5. Complete and submit the registration page and accept the terms of use.

6. The welcome page provides a link to the locally running application and displays the username and password to use for login. Navigate to [http://localhost:8000](http://localhost:8000) and, if prompted, log in with username `hue` and password `1111`.

#### A quick refresher on terminology

Recall that Pig deals with data in the form of _relations_, _bags_, _tuples_ and _fields_:

* A **field** is a typed data element, like `City of Chicago` (a `chararray`), `2012` (an `int`), or `3.1415` (a `double`).
* A **tuple** is an ordered set of fields notated with parentheses, like `(1, 2, 3)` or `(pi, 3.1415)`. Analogus to a row in a database.
* A **bag** is an unordered collection of tuples notated with braces, like `{(a, b), (1, 2)}`. Analogus to a table in a database.
* A **relation** is an outer bag. Given that bags can contain other bags (tuples can also contain other tuples) we call the outer-most bag the relation. Analogus to a database. 

Each collection type--bags and tuples--has a schema assocaited with it. A schema associates a type and alias ("name") with each element. Consider this example schema:

```
results: {dates:tuple(start_year:int,end_year:int),ages:tuple(name:chararray,age:int)}
```

From left to right:
* `results` is the alias (the name) of the relation
* `dates:tuple(...)` indicates that the relation contains a tuple called `dates`
* `(start_year:int, end_year:int)` denotes that the `dates` tuple contains two integers, called `start_year` and `end_year` respectively. 
* `ages:tuple(name:chararray,age:int)` indicates the relation also contains a second tuple called `ages` comprised of a string (`chararray`) called `name` and an integer called `age`.

You can print the schema of any alias with the command:
```DESCRIBE alias```

Note that when you do so, you may find that some elements in your schema are preceeded with an an identifier and two colons, for example, `tickets::ticket_count:int`. This identifier (`tickets::`) indicates the element's namespace; the alias from which it originated when joining or generating derived relations. Namespaces--as we'll see in the exercises--are important when dealing with relations containing elements that otherwise have the same name.

Importing the Ticket Data
-------------------------

All of the data required for this exercise can be found on the USB (under the `chicago-rlc-data` directory), or on Github ([https://github.com/defano/ccc-big-data/blob/master/chicago-rlc-data.zip](https://github.com/defano/ccc-big-data/blob/master/chicago-rlc-data.zip)). 

As is the case with virtually every introduction to big data, the example dataset we'll use in this course isn't big enough to really be considered "big data". Nonetheless, the tools and techniques we employ are scalable to datasets thousands of times the size of our paltry 250MB red light ticket log. That said, these big data tools are designed to scale; not to perform well in single-node, virtualized environments like ours. Running these analyses on the 250MB dataset will take quite some time. To reduce this write-run-debug cycle time, we've included subset data for the year 2012 (even with this subset, these tutorial scripts will take 5 minutes or more to execute). 

Of course, the following instructions will work equally well with the full dataset. The interested student is encouraged to repeat these exercises using the full set of red light camera data after succeeding with the 2012 subset.

#### Open HCatalog
We'll begin by using Apache's HCatalog to import and store our data on the platform. HCatalog provides a unified, relational view of data stored in a variety of formats like CSV or JSON. HCatalog abstracts the formatting details of the underlying data such that data analysis tools higher up the stack--like Pig, Hive or MapReduce--can operate on the data without concern for how its structured or formatted.

1. On the button bar at the top of the page, click the "HCat" icon. You'll be presented with the "HCatalog: Table List" page.

#### Import the ticket records
2. Click the "Create a new table from a file" link under in the actions panel on the left. As you do this, take note of the selected database (`default`); this is the database in which our table will be created. 
3. Name the table `rlc_all_tickets_2012` and provide a short description, something like `All red light tickets in 2012`. The table name will matter in future steps; the description will not.
4. Click the "Choose a file" button adjacent to the input file field. In the modal dialog that appears, click the "Upload a file" button, then navigate to and choose the `all_rlc_tickets_2012.txt` file on your filesystem.
5. As soon as the file has uploaded it will appear in the list of available files; click its link to select it.
6. The page will now display additional file import options and a preview of the table that will be created. HCatalog will auto-detect most of the file structure options for us (like encoding and delimiters). Leave the default selections as they are. 
7. In the "Table Preview" section of the page we find the HCatalog has inferred data types for each column, but we  need to provide more meaningful column names:
  - Change the first column name from `7000634986` to `ticket_id`
  - Change the second column name from `2007_01_01` to `date`
  - Change the third column name from `y301669` to `license_plate_number`
  - Change the fourth column name from `pas` to `license_plate_type`
  - Change the fifth column name from `il` to `license_plate_state`
  - Change the sixth column name from `1900_n_ashland_ave` to `camera_address`
8. Click the "Create Table" button. The operation may take a few minutes to complete. When complete the page return to "HCatalog: Table List" and you'll note the presence of our newly created table, `all_rlc_tickets_2012`, in the list. Congratulations! You've just created your first HCatalog table!

#### Import the appeal records

We'll create a second table representing each ticket appeal attempt following the same steps we used for the ticket records:

1. Click the "Create a new table from a file" link under in the actions panel on the left.
2. Name the table `admin_hearing_results_2012` and provide a short description.
3. Click the "Choose a file" button, then "Upload a file." Find and select the `admin_hearing_results_2012.txt` file on your filesystem.
4. Once the upload completes, click the table to display the file import options; leave the default selections as they are, but rename the columns accordingly:
  - Change the first column to `ticket_id`
  - Change the second column to `issue_date`
  - Change the third column to `hearing_date`
  - Change the fourth column name `result`
5. Click the "Create Table" button and wait a few minutes while the table is created.

Analyzing the Data
------------------

In this tutorial, we'll be using Apache Pig to crunch our data. Pig is a scripting language that enables data scientists to analyze datasets using a reasonably simple scripting language (called, no less, *Pig Latin*) without regard to the reasonably complex, underlying map-reduce architecture. Pig compiles Pig Latin scripts into one or more map-reduce jobs that execute in the Hadoop environment. Think of map-reduce as Big Data's assembly language and Pig Latin as Big Data's C.

#### Figure out which cameras generate the most revenue

We'll start with a fairly simple task: For each camera (identified by its address), count the number of tickets issued by that camera in 2012. 

The algorithm we'll follow for doing so is:

* Load the ticket records
* Group the tuples based on camera address
* For each group, count the number of tickets issued
* Output a list (*bag*) of tuples consisting of `(camera address, ticket count)`

_**A note to the pedantic:** Clearly, each intersection has multiple cameras installed to capture traffic moving in different directions. In the context of this lab the term "camera" will refer logically to all the physical cameras installed at a given intersection._

1. Start by clicking the "Pig" icon in the button bar at the top of the page. 
2. Give our script a name by entering something like `CountTickets` into the `Title` field.
3. In the script editor field, enter the following statement to tell Pig to load the `all_rlc_tickets_2012` dataset into a relation called `tickets` (its *alias*). You may also find it helpful to use the "Pig Helper" drop-down menu to automatically populate the right-hand side of this expression (look under the "HCatalog" submenu).

        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();

4. Our first transformation on the `tickets` data will be to group the records (tuples) by camera address. The goal is to produce a relation (bag) in which each tuple in the bag contains two fields: the camera address aliased as `group` and each corresponding record aliased with the original relation's name (`tickets`, in our case). Enter this statement after the previous line in your script: 

        cameras = GROUP tickets BY camera_address;

The result of this will be to produce a new relation with the schema cameras: `{group:chararray, tickets:{...}}`.

5. For each group (i.e., for each different red light camera in Chicago), produce a bag containing the camera address, the number of tickets issued, and the number of tickets issued multiplied by the fine for each ticket ($100).

        results = FOREACH cameras GENERATE 
	        group AS camera_address, 
	        COUNT(tickets) AS ticket_count, 
	        COUNT(tickets) * 100 AS revenue;

 * This statement will produce a `results` alias that should contain a relation with the schema `{camera_address:chararray, ticket_count:int, revenue:int}`
 * For completeness and illustration, we're assigning field names (i.e., `AS camera_address`, `AS ticket_count`, `AS revenue`) to the data we're generating in the resulting relation. Since we don't need to refer to these fields in the future, this is isn't necessary; the `as...` clauses could be removed without harm. 
 * The `COUNT` operator returns the number of non-null elements in the specified bag. The intent here is to report the number of rows/elements in the group. 
 
6. Now, lets order the `results` bag by `revenue` so that we can quickly identify those cameras producing the greatest revenue:

        ordered_results = ORDER results BY revenue DESC;
        
7. Finally, dump our `ordered_results` to output:

        DUMP ordered_results;

8. Check your results: your finished Pig script should look like...

        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();
        cameras = GROUP tickets BY camera_address;
        results = FOREACH cameras GENERATE 
            group as camera_address, 
            COUNT(tickets) as ticket_count, 
            COUNT(tickets) * 100 as revenue;
        ordered_results = ORDER results BY revenue DESC;
        DUMP ordered_results;

#### Execute your first Pig script

First and foremost: Check, double-check, and triple-check your script for errors and typos. It can take Pig a few minutes to catch obvious syntax errors--having to wait several minutes to figure out you forgot a comma is painful! Worse yet, semantic errors (like referencing a non-existant field in a relation) won't be caught until the statement executes at runtime. With sizeable datasets this could be hours from now! When programming Pig, it's well worth the time investment to review your code before you run it. 

When you're sure your script looks good:

1. Click "Execute" to submit the job.
2. As the job runs, note the presence of the blue progress bar that appears directly below the script editor field.
  - For a more detailed view of whats going on, open the "Job Browser" (the construction hard-hat icon) in a new tab. You'll note that by executing your script, a "TempletonControllerJob" has started on the platform. This controller job manages your script execution and is responsible for starting the child map-reduce jobs resulting from operations specified in your script.
  - If you close the script editor window or wish to see results from previously executed jobs, click the "Query History" link at the top of the Pig page. 
3. Your script will probably take 2-5 minutes to execute, depending on your machine. Provided your script executed successfully, the output (which will appear below the script editor field) should be:

        (4200 S CICERO AVENUE,19800,1980000)
        (400 W BELMONT AVE,15076,1507600)
        (30 W 87TH STREET,12376,1237600)
        (400 S WESTERN AVENUE,12081,1208100)
        ...

It should be obvious that the first element in the tuple is the camera address (i.e., `4200 S CICERO AVENUE`); the second element is the number of tickets issued (`19800`) and the third element is the total revenue collected by the city (assuming a $100 fine with no tickets overturned in court). 

Congratulations on your first Pig Latin script!

#### Find cameras and dates that issued an abnormally large number of tickets

Next, lets see if we can identify dates on which camera's issued an abnormally large number of tickets. For the purposes of this exercise, we're going to define _abnormally large_ as any date on which the number of tickets issued by a camera are in the 99th percentile of tickets issued (i.e., fewer than 1% of all days produce more tickets for the given camera).

Our algorithm is:
* Count the number of tickets issued per camera, per date
* Determine how many tickets constitute the 99th percentile for each camera
* Join the 99th percentile data with the number of tickets per camera per day
* Filter the result, eliminating records where the number of tickets issued is fewer than the 99th percentile. 

We'll be making use of a LinkedIn-authored _user defined function_, (UDF) called "DataFu" in this exercise to simplify the calculation percentiles/quantiles. UDFs provide an extension to the Pig Latin language and can be written in Java, Python and Javascript. Authoring UDFs is relatively straightforward, although the details are beyond the scope of this lab.

1. Create a new script by clicking the "New Script" link on the page. Give the new script a name like `Outliers`.
2. Upload the DataFu UDF to the Hortonworks platform by clicking the "Upload UDF Jar" button. Locate the `datafu-1.2.0.jar` library under the `lib` directory of the USB (or on GitHub, [here](https://github.com/defano/ccc-big-data/blob/master/lib/datafu-1.2.0.jar)).
3. In order to make use of this library inside our Pig Latin script, we need to tell Pig about it and, as a convenience, assign an alias to the function's name (so as not to have to refer to the method using its fully-qualified package name):

        REGISTER datafu-1.2.0.jar
        DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.99');
        
4. Like our last script, we only need to load the ticket data:

        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();
        
5. Our first transformation on the `tickets` data will be to group all records (tuples) by camera address and date. Each group then represents the activity of a given camera on a given date. To determine the number of tickets issued by the camera on the date we simply count number of elements in the group:

        tickets_by_address_date = FOREACH (GROUP tickets BY (camera_address, date)) GENERATE 
        	group.camera_address AS camera_address, 
            group.date AS date, 
            COUNT(tickets) AS ticket_count;    

Finally, your script should look like:

        REGISTER datafu-1.2.0.jar
        DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.99');

        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();

        tickets_by_address_date = FOREACH (GROUP tickets BY (camera_address, date)) GENERATE 
        	group.camera_address AS camera_address, 
            group.date AS date, 
            COUNT(tickets) AS ticket_count;    
    
        quantiles_by_address = FOREACH (GROUP tickets_by_address_date BY (camera_address)) GENERATE
        	group AS camera_address, 
            Quantile(tickets_by_address_date.(ticket_count)) AS quantile_99,
            AVG(tickets_by_address_date.(ticket_count)) AS average;

        tickets_quantiles = JOIN tickets_by_address_date BY camera_address, quantiles_by_address BY camera_address;
        outliers = FILTER tickets_quantiles BY (ticket_count > quantile_99.($0));

        DUMP outliers; 

#### Count the appeal results by camera

#### Find cameras and dates that produced abnormal appeal success
