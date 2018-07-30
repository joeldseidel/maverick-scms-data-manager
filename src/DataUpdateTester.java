import org.json.JSONObject;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class DataUpdateTester
{
    static BlockingQueue<String> objectStringQueue = new LinkedBlockingQueue();
    static BlockingQueue<JSONObject> jsonObjectQueue = new LinkedBlockingQueue<>();

    private static final String localDataFile = System.getProperty("user.home") +"/fda_data_files";

    public static void main(String[] args)
    {
        System.out.println("Started: " + LocalDateTime.now());
        LocalDataDownloader.tryFetchFDAFiles();

        //initialize consumer threads
        Thread parserThread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser2Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser3Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser4Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser5Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser6Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser7Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser8Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser9Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser10Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser11Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser12Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser13Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser14Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser15Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser16Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser17Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser18Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser19Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser20Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser21Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser22Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser23Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser24Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser25Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser26Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser27Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser28Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread parser29Thread = new Thread(new ParsingThread(objectStringQueue, jsonObjectQueue));
        Thread commitThread = new Thread(new CommitThread(jsonObjectQueue));
        Thread commit2Thread = new Thread(new CommitThread(jsonObjectQueue));
        Thread commit3Thread = new Thread(new CommitThread(jsonObjectQueue));
        Thread commit4Thread = new Thread(new CommitThread(jsonObjectQueue));
        Thread commit5Thread = new Thread(new CommitThread(jsonObjectQueue));

        try {
            Thread.sleep(10000);
        }
        catch (InterruptedException iee)
        {
            System.out.println("Reader thread killed while waiting");
        }
        parserThread.start();
        parser2Thread.start();
        parser3Thread.start();
        parser4Thread.start();
        parser5Thread.start();
        parser6Thread.start();
        parser7Thread.start();
        parser8Thread.start();
        parser9Thread.start();
        parser10Thread.start();
        parser11Thread.start();
        parser12Thread.start();
        parser13Thread.start();
        parser14Thread.start();
        parser15Thread.start();
        parser16Thread.start();
        parser17Thread.start();
        parser18Thread.start();
        parser19Thread.start();
        parser20Thread.start();
        parser21Thread.start();
        parser22Thread.start();
        parser23Thread.start();
        parser24Thread.start();
        parser25Thread.start();
        parser26Thread.start();
        parser27Thread.start();
        parser28Thread.start();
        parser29Thread.start();
        commitThread.start();
        commit2Thread.start();
        commit3Thread.start();
        commit4Thread.start();
        commit5Thread.start();

        Scanner kybd = new Scanner(System.in);
        System.out.println("Download successful, hit enter to begin update");
        kybd.nextLine();
        System.out.println("Start Time: " + LocalDateTime.now());

        //start reading the files
        Thread readerThread = new Thread(new ReaderThread(objectStringQueue));
        readerThread.start();


        while (true)
        {
            System.out.println("Time: " + LocalDateTime.now());
            System.out.println("Object Length: " + objectStringQueue.toArray().length);
            System.out.println("Parsed Length: " + jsonObjectQueue.toArray().length);
            try
            {
                Thread.sleep(10000);
            }
            catch (InterruptedException IEE)
            {
                System.out.println("Interrupt on Main Thread");
            }

        }
    }
}
