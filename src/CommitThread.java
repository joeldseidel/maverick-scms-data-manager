//Author:   Erik Abramczyk
//Created:  July 26, 2018
//Purpose:  Single thread that will churn through the parsedObjects queue, committing objects to the database

import java.util.concurrent.BlockingQueue;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


public class CommitThread implements Runnable
{
    private BlockingQueue<JSONObject> parsedObjects;

    public CommitThread(BlockingQueue<JSONObject> parPointer)
    {
        parsedObjects = parPointer;
    }

    public void run()
    {
        while(true)
        {
            try
            {
                //take objects off the queue to make it not so large
                JSONObject object = parsedObjects.take();
                object = null;
            }
            catch (InterruptedException IEE)
            {
                System.out.println("Commit Thread Interrupted");
            }
        }
    }


}
