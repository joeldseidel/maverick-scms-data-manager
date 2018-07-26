//Author:       Erik Abramczyk
//Date Created: July 26, 2018
//Purpose:      Encapsulate all Reading of FDA device file documentation into a single thread
//                  in order to allow multi-threading of FDA File parsing

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class ReaderThread implements Runnable
{
    private BlockingQueue<String> objectStrings;
    //local path for all downloaded files
    private final String localDataFile = System.getProperty("user.home") +"/fda_data_files";

    //initialize the ReaderThread with the passed pointer
    public ReaderThread(BlockingQueue<String> pointer)
    {
        objectStrings = pointer;
    }

    public void run()
    {
        System.out.println("Starting");
        splitAllFiles();
        System.out.println("finished");
    }

    //Parse all of the files given from the FDA after a fetch
    private void splitAllFiles()
    {
        //Get all local storage file contents (this will be all the data files)
        File dataFilesList[] = new File(localDataFile).listFiles();
        for(int i = 0; i < dataFilesList.length; i++)
        {
            System.out.println("Working on parsing data file " + (i + 1) + "/" + dataFilesList.length);
            //It's going to be the only file in its parent, just get the path from it
            System.out.println("Data File Path: " + dataFilesList[i].getPath());
            File thisDataFilePath = new File(dataFilesList[i].getPath());
            splitSingleFile(thisDataFilePath);
        }
    }

    //parse a single file from the FDA into the objectStrings queue
    private void splitSingleFile(File parseFromFile)
    {
        int objectPut = 0;
        //attempt to make a Reader connection to the File
        try(BufferedReader br = new BufferedReader(new FileReader(parseFromFile)))
        {
            System.out.println("Reading File");
            String readObject = "";
            String thisLine;
            int openObjectCount = 0;
            int closedObjectCount = 0;

            //read past the lines until we reach the line specifying "results" will follow
            while ((thisLine = br.readLine()).contains("\"results\": [") == false)
            {
                System.out.println("Skipping: " + thisLine);
            };

            //read all results of the file
            while ((thisLine = br.readLine()) != null)
            {
                //keep track of the number of objects that have been made open and closed
                if(thisLine.contains("{"))
                {
                    openObjectCount++;
                    //System.out.println("Found an open object");
                }
                if(thisLine.contains("}"))
                {
                    //System.out.println("Found a closed object");
                    closedObjectCount++;
                }
                //System.out.println("Appending: " + thisLine);
                //System.out.println("Open Objects: " + openObjectCount);
                //System.out.println("Closed Objects: " + closedObjectCount);
                readObject += thisLine;
                //when there were open objects and now all open objects have been closed, commit this string to the
                //queue for parsing
                if (openObjectCount != 0 && openObjectCount == closedObjectCount)
                {
                    System.out.println("putting an object");
                    objectStrings.put(readObject);
                    openObjectCount = 0;
                    closedObjectCount = 0;
                    objectPut++;
                }
            }
            System.out.println("Finished Reading: " + objectPut + " objects put on the queue");
        }
        catch (InterruptedException IEE)
        {
            System.out.println("Thread interrupted while trying to append to parsing queue");
        }
        catch(IOException ioE)
        {
            System.out.println("Error while attempting to read FDA File Line");
        }
    }
}
