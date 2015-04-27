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

We'll be using the Hortonworks Sandbox v2.2.4 as our "big data" platform during this exercise. The sandbox is freely available, installs trivially, and provides a user friendly interface (it even has its own built-in tutorial which you can use to improve your skillset outside this course). 

Most host machines should have little trouble running the sandbox, but official requirements from Hortonworks state:

* Windows XP, Windows 7, Windows 8 or Mac OS X
* Minimum 4GB RAM; 8GB required to run Ambari and Hbase
* Virtualization enabled on BIOS
* Browser: Chrome 25+, IE 9+, Safari 6+ recommended. (Sandbox will not run on IE 10)

Install and run the virtual machine:

1. Locate the VirtualBox software on the provided thumb drive and install it on your Mac or PC.

2. Import the Hortonworks virtual machine (OVA) into VirtualBox by double-click the `.ova` file on the thumb drive. Note that the import process may take several minutes. 

3. Once imported, click the "Start" button to run the virtual machine. 
   
4. The Hortonworks sandbox runs as a web app; the UI front-end is a Hortonworks development called "Hue". As soon as the virtual machine has booted its console will display the URL to access the application. Open a browser and navigate to that URL: http://127.0.0.1:8888

5. Complete and submit the registration page and accept the terms of use.

6. The welcome page provides a link to the locally running application and displays the username and password to use for login. Navigate to [http://localhost:8000](http://localhost:8000) and, if prompted, log in with username `hue` and password `1111`.

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
7. In the "Table Preview" section of the page we find the HCatalog has inferred data types for each column, but we do need to provide more meaningful column names and review the types:
  - Change the first column name from `7000634986` to `ticket_id`
  - Change the second column name from `2007_01_01_00_02_00` to `timestamp` and change the type to `String`
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

#### Start the Pig editor in Hue

1. Start by clicking the "Pig" icon in the button bar at the top of the page. 

#### Figure out which cameras generate the most revenue

We'll start with a fairly simple task: For each camera (identified by its address), count the number of tickets issued by that camera in 2012. 

The algorithm we'll follow for doing so is:

* Load the ticket records
* Group the tuples based on camera address
* For each group, count the number of tickets issued
* Output a list (*bag*) of tuples consisting of `(camera address, ticket count)`

_**A note to the pedantic:** Clearly, each intersection has multiple cameras installed to capture traffic moving in multiple directions. In the context of this lab the term "camera" will refer logically to all the physical cameras installed at a given intersection._

1. Start by giving our script a name by entering something like `CountTickets` into the `Title` field.
2. In the Pig script editor, enter the following statement to tell Pig to load the `all_rlc_tickets_2012` dataset into a variable (*alias*) called `tickets`. You may also find it helpful to use the "Pig Helper" drop-down menu to automatically populate the right-hand of this expression. It can be found under the "HCatalog" submenu.

        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();

3. Next, we want to group tuples by `camera_address`, then, for each group, output a tuple containing the camera address, the number of tickets issued, and the number of tickets issued multiplied by the fine for each ticket ($100).

        results = FOREACH (GROUP tickets BY camera_address) GENERATE 
	        group as camera_address, 
	        COUNT(tickets.(ticket_id)) as ticket_count, 
	        COUNT(tickets.(ticket_id)) * 100 as revenue;

 * This statement will produce a `results` alias that should contain a relation with the schema `{camera_address:String, ticket_count:Integer, revenue:Integer}`
 * For completeness and illustration, we're assigning field names (i.e., `as camera_address`, `as ticket_count`, `as revenue`) to the data we're generating in the resulting relation. Since we don't need to refer to these fields in the future, this is isn't necessary; the `as...` clauses could be removed without harm. 
 * The `COUNT` operator returns the number of non-null elements in the specified field. The intent here is to report the number of rows/elements in the group. 
 
4. Now, lets order the `results` tuple by `revenue` so that we can quickly identify those cameras producing the greatest revenue:

        ordered_results = ORDER results BY revenue DESC;
        
5. Finally, dump the `ordered_results` to output:

        DUMP ordered_results;

6.   Your final Pig script should look like...

        tickets = LOAD 'default.all_rlc_tickets_2012' USING org.apache.hcatalog.pig.HCatLoader();
        results = FOREACH (GROUP tickets BY camera_address) GENERATE 
            group as camera_address, 
            COUNT(tickets.(ticket_id)) as ticket_count, 
            COUNT(tickets.(ticket_id)) * 100 as revenue;
        ordered_results = ORDER results BY revenue DESC;
        DUMP ordered_results;

7. Check, double-check and triple-check your script for errors and typos. (Having to wait several minutes to figure out you forgot a comma is painful!). When you're sure everything looks good, click "Execute".

Provided your script executed successfully, your output should look like:

```
(4200 S CICERO AVENUE,19800,1980000)
(400 W BELMONT AVE,15076,1507600)
(30 W 87TH STREET,12376,1237600)
(400 S WESTERN AVENUE,12081,1208100)
...
```

It should be obvious that the first element in the tuple is the camera address (i.e., `4200 S CICERO AVENUE`); the second element is the number of tickets issued (`19800`) and the third element is the total revenue collected by the city (assuming a $100 fine with no tickets overturned in court). 

#### Count the appeal results by camera
#### Find cameras and dates that produced abnormal appeal success
