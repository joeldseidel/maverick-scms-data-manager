//Author:       Erik Abramczyk
//Date Created: July 26, 2018
//Purpose:      Thread implenetation of popping values off queue of objects to parse, parse the string into a JSONObject
//                  instance, and push that instance onto the commit queue for DB commit

import java.util.concurrent.BlockingQueue;
import org.json.JSONObject;

public class ParsingThread implements Runnable
{
    private BlockingQueue<String> objectStrings;
    private BlockingQueue<JSONObject> parsedObjects;

    public ParsingThread(BlockingQueue<String> objPointer, BlockingQueue<JSONObject> parPointer)
    {
        objectStrings = objPointer;
        parsedObjects = parPointer;
    }

    public void run()
    {
        while(true)
        {
            //pop object off of objectStrings queue accounting for possible interrupting which should occur when
            //these threads are killed when the files are finished
            String newObjectString = "";
            try
            {
                if (objectStrings.toArray().length == 0)
                {
                    //yield to other threads when there are no more to parse
                    Thread.yield();
                }
                newObjectString = objectStrings.take();
            }
            catch (InterruptedException IEE)
            {
                System.out.println("Wait for an object to parse interrupted");
                return;
            }
            try
            {
                //parse JSON string into a JSONObject instance
                JSONObject newJSONObject = new JSONObject(newObjectString);
                //push JSONObject onto the parsedObjects queue for DB commit
                parsedObjects.put(newJSONObject);
            }
            catch (InterruptedException IEE)
            {
                System.out.println("Wait to place an object on the parsed queue interrupted on object: \n" + newObjectString);
            }
            catch (org.json.JSONException json)
            {
                System.err.println("JSON String Invalid: " + newObjectString);
            }
        }
    }
}
