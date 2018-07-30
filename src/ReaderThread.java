//Author:       Erik Abramczyk
//Date Created: July 26, 2018
//Purpose:      Encapsulate all Reading of FDA device file documentation into a single thread
//                  in order to allow multi-threading of FDA File parsing

import java.io.LineNumberReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
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
        //sort them so it has a predictable execution path (file 1-13 in order)
        Arrays.sort(dataFilesList);
        for(int i = 0; i < dataFilesList.length; i++)
        {
            System.out.println("Working on parsing data file " + (i + 1) + "/" + dataFilesList.length);
            //It's going to be the only file in its parent, just get the path from it
            System.out.println("Data File Path: " + dataFilesList[i].getPath());
            File thisDataFilePath = new File(dataFilesList[i].getPath());
            //skip hidden files (caused bug)
            if (thisDataFilePath.getName().substring(0,1).equals("."))
            {
                continue;
            }
            splitSingleFile(thisDataFilePath);
        }
    }

    //parse a single file from the FDA into the objectStrings queue
    private void splitSingleFile(File parseFromFile)
    {
        //attempt to make a Reader connection to the File
        try(LineNumberReader br = new LineNumberReader(new FileReader(parseFromFile), 200000))
        {

            System.out.println("Reading File: " + parseFromFile.getName());
            String readObject = "";
            String thisLine;
            //keep track of the current open JSON objects read in, commit when all open objects have been closed
            int openObjectCount = 0;
            int closedObjectCount = 0;

            //read past the lines until we reach the line specifying "results" will follow
            while ((thisLine = br.readLine()).contains("\"results\": [") == false) { }

            //read all results of the file
            while ((thisLine = br.readLine()) != null)
            {
                String trimmed = thisLine.trim();
                //keep track of the number of objects that have been made open and closed
                //ensure the curly brace is in a position where it means an actual object open or close,
                //extra checks are for fixing a bug where one vendor has curly braces in some device names
                if(trimmed.equals("{") || (trimmed.length() > 2 && trimmed.substring(trimmed.length() - 1, trimmed.length()).equals("{")))
                {
                    openObjectCount++;
                }
                if(trimmed.equals("}") || trimmed.equals("},"))
                {
                    closedObjectCount++;
                }

                //append this line to the JSON object string
                readObject += thisLine;
                //when there were open objects and now all open objects have been closed, commit this string to the
                //queue for parsing
                if (openObjectCount != 0 && openObjectCount == closedObjectCount)
                {
                    //trim to ensure no white space before open brace, which would cause a JSON parsing failure
                    objectStrings.put(readObject.trim());
                    //reinitialize the out-of-loop variables
                    openObjectCount = 0;
                    closedObjectCount = 0;
                    readObject = "";
                }
            }
            System.out.println("Finished Reading: " + parseFromFile.getName());
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
