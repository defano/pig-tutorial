Chicago Coders Conference Big Data Hands on Lab
===============================================

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

All of the data required for this exercise can be found on the USB (under the `chicago-rlc-data` directory), or on Github ([https://github.com/defano/ccc-big-data/blob/master/chicago-rlc-data.zip](https://github.com/defano/ccc-big-data/blob/master/chicago-rlc-data.zip)). As is the case with virtually every introduction to big data, the example dataset we'll use in this course isn't big enough to really be considered "big data". Nonetheless, the tools and techniques we employ are scalable to datasets thousands of times the size of our paltry 250MB red light ticket log.

#### Open HCatalog
We'll begin by using Apache's HCatalog to import and store our dataset on the platform. HCatalog provides a unified, relational view of data stored in a variety of formats like CSV or JSON. The intent of the project is to abstract the formatting details of the underlying data such that data analysis tools higher up the stack--like Pig, Hive or MapReduce--can operate on the data without concern for how its structured or formatted on disk.

1. On the button bar at the top of the page, click the "HCat" icon. You'll be presented with the "HCatalog: Table List" page.

#### Import the ticket records
2. Click the "Create a new table from a file" link under in the actions panel on the left. As you do this, take note of the selected database (`default`); this is the database in which our table will be created. 
3. Name the table `rlc_all_tickets` and provide a short description, something like `All red light tickets`. The table name will matter in future steps; the description will not.
4. Click the "Choose a file" button adjacent to the input file field. In the modal dialog that appears, click the "Upload a file" button then navigate to and choose the `all_rlc_tickets.txt` file on your filesystem.
5. As soon as the file has uploaded it will appear in the list of available files; click its link to select it.
6. The page will now display additional file import options and a preview of the table that will be created. HCatalog will auto-discover most of the file structure options for us (like encoding and delimiters). Leave the default selections as they are. 
7. In the "Table Preview" section of the page we find the HCatalog has (correctly) inferred data types for each column, but we do need to provide more meaningful column names:
  - Change the first column name from `7000634986` to `TICKET_ID`
  - Change the second column name from `2007_01_01_00_02_00` to `TIMESTAMP`
  - Change the third column name from `y301669` to `LICENSE_PLATE_NUMBER`
  - Change the fourth column name from `pas` to `LICENSE_PLATE_TYPE`
  - Change the fifth column name from `il` to `LICENSE_PLATE_STATE`
  - Change the sixth column name from `1900_n_ashland_ave` to `CAMERA_ADDRESS`
8. Click the "Create Table" button. The operation may take a few minutes to complete. When complete the page return to "HCatalog: Table List" and you'll note the presence of our newly created table, `all_rlc_tickets`, in the list. Congratulations! You've just created your first HCatalog table!

#### Import the appeal records

We'll create a second table representing each ticket appeal attempt following the same steps we used for the ticket records:

1. Click the "Create a new table from a file" link under in the actions panel on the left.
2. Name the table `admin_hearing_results` and provide a short description.
3. Click the "Choose a file" button, then "Upload a file." Find and select the `admin_hearing_results.txt` file on your filesystem.
4. Once the upload completes, click the table to display the file import options; leave the default selections as they are, but rename the columns accordingly:
  - Change the first column to `TICKET_ID`
  - Change the second column to `ISSUE_DATE`
  - Change the third column to `HEARING_DATE`
  - Change the fourth column name `RESULT`
5. Click the "Create Table" button and wait a few minutes while the table is created.



hearings = LOAD 'default.admin_hearing_results' USING org.apache.hive.hcatalog.pig.HCatLoader();
tickets = LOAD 'default.all_rlc_tickets' USING org.apache.hive.hcatalog.pig.HCatLoader();

joined = JOIN hearings BY TICKET_ID, tickets BY TICKET_ID; 
DUMP joined;