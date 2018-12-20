package hdfs;

import junit.framework.TestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DataNodeTest extends TestCase {

    private DataNode dataNode;
    private String getContent, addContent;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.dataNode = new DataNode("./data");
        this.getContent = new String(Files.readAllBytes(Paths.get("./data/test.0")));
        this.addContent = new String(Files.readAllBytes(Paths.get("./data/filesample.txt")));
        if (Files.notExists(Paths.get("./data/test.2"))) {
            Files.createFile(Paths.get("./data/test.2"));
        }
    }

    public void tearDown() throws Exception {
        if (!Files.notExists(Paths.get("./data/test.1"))) {
            Files.delete(Paths.get("./data/test.1"));
        }
        if (!Files.notExists(Paths.get("./data/test.2"))) {
            Files.delete(Paths.get("./data/test.2"));
        }
    }

    public void testGetChunk() {
        try {
            assertEquals(this.getContent, this.dataNode.getChunk("test", 0));
        } catch (FileNotFoundException e) {
            fail("Not found test.0");
        } catch (IOException e) {
            fail("I/O exception: " + e.getMessage());
        }
    }

    public void testAddChunk() {
        try {
            this.dataNode.addChunk("test", 1, this.addContent);
        } catch (IOException e) {
            fail("Writing error: " + e.getMessage());
        }
        assertFalse("Not found writing data", Files.notExists(Paths.get("./data/test.1")));
    }

    public void testDelChunk() {
        try {
            this.dataNode.delChunk("test", 2);
        } catch (FileNotFoundException e) {
            fail("Not found file to delete");
        } catch (IOException e) {
            fail("File impossible to delete: " + e.getMessage());
        }
        assertTrue("Not found writing data", Files.notExists(Paths.get("./data/test.2")));
    }
}