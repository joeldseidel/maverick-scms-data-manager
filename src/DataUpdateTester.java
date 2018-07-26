import org.json.JSONObject;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DataUpdateTester
{
    static BlockingQueue<String> objectStringQueue = new LinkedBlockingQueue();
    static BlockingQueue<JSONObject> jsonObjectQueue = new LinkedBlockingQueue<>();

    public static void main(String[] args)
    {
        LocalDataDownloader.tryFetchFDAFiles();
        Thread readerThread = new Thread(new ReaderThread(objectStringQueue));
        //Thread parserThread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));

        readerThread.start();
        try {
            Thread.sleep(10000);
        }
        catch (InterruptedException iee)
        {
            System.out.println("Reader thread killed while waiting");
        }
    }
}
