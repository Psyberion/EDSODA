# EDSODA (ED Ships Onboard Data Acquisition) #

### About This Repository ###

* EDSODA Version: 1.0.23-a (Elite Dangerous Ships Onboard Data Acquisition)
* EDSODA is a NetCore 3.1 application which parses all of a player's Elite Dangerous journal files, containing past and also current details of events as they occur in the game, and stores the information in a relational database (see the EDSODS repository).
* EDSODA is part of the EDSIS (Elite Dangerous Ships Integrated Systems) project. This project aims to enhance the player experience, with added access to bespoke transformed data from both the players journal files and external sources, for a greater level of immersion.
* Currently the EDSODA application has been developed so far to run on Windows 10. But because the application is designed to be running on the same hardware on which Elite Dangerous is being played, the aim is to have this working on Linux and MacOs as well.
* EDSODA *requires* a configured instance of the EDSODS MySQL database, available in the EDSODS repository, as it will need to connect to this to start building the player's database of related journal data. EDSODS does not have to be hosted on the same machine as EDSODA, the database can be hosted on another machine, even on another operating system, providing the machine running EDSODA is able to connect to EDSODS. Personally, I run EDSODA on my Windows 10 PC where I run Elite Dangerous, and I have EDSODS running on a networked Linux box where I also run the interface software, EDSOM, for quering and displaying the data.

### Set Up ###

* Summary of set up

The application comprises the EDSODA.sln solution file and the EDSODA project folder and its contents. It is suggested that Visual Studio 2019 is used to build the executable application, this includes the Community version of Visual Studio 2019.

* Configuration

The EDSODA project folder contains an example configuration file, named 'App.config.example'. It is suggested that you make a copy of this file and name it 'App.config' in the same location. Open the new App.config file in Visual Studio or a text editor, and edit the settings as described below.

In the &lt;connectionStrings&gt;...</connectionStrings> section there is an entry named 'EDSODS', this refers to the Elite Dangerous Ships Onboard Data Store and is essentially where all the parsed journal event data will be stored. You will need to visit the EDSODS repository for details on setting up that MySQL database ready for use by EDSODA and other parts of the EDSIS project. The example App.config file provides one possible set of connection details with example values, but you can change this to provide your own valid connection details for your own EDSODS MySQL instance.

In the <appSettings>...</appSettings> section there are a couple of Key/Value settings...

Please set the value of the 'journalPath' setting to the path of where your Elite Dangerous journal files are located. For Elite Dangerous running on Windows 10, this is normally under "C:\Users\<username>\Saved Games\Frontier Developments\Elite Dangerous", where <username> should be replaced with your Windows username.

Under normal circumstances the 'ReprocessEvents' setting should have an empty value. This setting is used to instruct the application to re-parse all previous events of a certain type and store them in the database. So, if you were to accidentally corrupt the data in one of the EDSODS tables, perhaps you wanted to update a record manually in the FSDJump table and ended up updating all the records by mistake. You could truncate the FSDJump table, clearing all the records in that table, then set the value for the 'ReprocessEvents' setting to "FSDJump" and then run the application. In doing this, the application would go through all of the stored events of type "FSDJump", and essentially repopulate the table. However, this will take time depending on how much data there is for that event type, and most importantly you must clear the value in the 'ReprocessEvents' setting afterwards, otherwise the reprossessing will occur again the next time you run the application.

* Dependencies

The EDSODA application depends on a few things...

You must have an instance of the EDSODS MySQL database set up and accessible to EDSODA. You must also have the connection string set up in the App.config file, in order for the application to access that EDSODS database instance. You must also configure the 'journalPath' setting in the App.config file with the location of your Elite Dangerous folder containing your journal files.

The application requires that the NuGet packages MySql.Data (8.0.23) and Newtonsoft.Json (12.0.3) are installed.

* Database configuration

The database requirements and configuration details should be found in the EDSODS repository.

* How to run tests

Tests are not included at this time.

* Deployment instructions

The application executable and config file will be located in the EDSODA\bin\Debug or EDSODA\bin\Release folders once you build the solution/project in Visual Studio. The application can be run directly from there or copied to an appropriately named folder under "C:\Program Files" if preferred. There is currently no specific deployment process.

### Contact ###

* Repo owner/admin
