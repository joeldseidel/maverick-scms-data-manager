public class FDAFile
{
    private String fileName;
    private String disposition;
    private String contentType;
    private int contentLength;

    FDAFile(String fileName, String disposition, String contentType, int contentLength)
    {
        this.fileName = fileName;
        this.disposition = disposition;
        this.contentType = contentType;
        this.contentLength = contentLength;
    }
    String getFileName()
    {
        return this.fileName;
    }

    String getDisposition()
    {
        return this.disposition;
    }

    String getContentType()
    {
        return this.contentType;
    }

    int getContentLength()
    {
        return this.contentLength;
    }
}
