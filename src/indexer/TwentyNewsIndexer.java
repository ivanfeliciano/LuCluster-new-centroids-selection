/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


/**
 *
 * @author Debasis
 */

interface NewsFields {
    String FIELD_DOCNO = "docno";
}

public class TwentyNewsIndexer {
    Properties prop;
    File indexDir;
    IndexWriter writer;
    Analyzer analyzer;
    List<String> stopwords;
    Boolean flag = false;
    int docIdx = 0;

    protected List<String> buildStopwordList(String stopwordFileName) {
        List<String> stopwords = new ArrayList<>();
        String stopFile = prop.getProperty(stopwordFileName);        
        String line;

        try (FileReader fr = new FileReader(stopFile);
            BufferedReader br = new BufferedReader(fr)) {
            while ( (line = br.readLine()) != null ) {
                stopwords.add(line.trim());
            }
            br.close();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return stopwords;
    }

    Analyzer constructAnalyzer() {
        Analyzer eanalyzer = new EnglishAnalyzer(
            StopFilter.makeStopSet(buildStopwordList("stopfile"))); // default analyzer
        return eanalyzer;
    }
    
    public TwentyNewsIndexer(String propFile) throws Exception {
        prop = new Properties();
        prop.load(new FileReader(propFile));                
        analyzer = constructAnalyzer();            
        String indexPath = prop.getProperty("index");        
        indexDir = new File(indexPath);
    }
    
    public Analyzer getAnalyzer() { return analyzer; }

    void processAll() throws Exception {
        System.out.println("Indexing 20 news collection...");
        
        IndexWriterConfig iwcfg = new IndexWriterConfig(analyzer);
        iwcfg.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        writer = new IndexWriter(FSDirectory.open(indexDir.toPath()), iwcfg);
        
        indexAll();
        
        writer.close();
    }

    void indexAll() throws Exception {
        if (writer == null) {
            System.err.println("Skipping indexing... Index already exists at " + indexDir.getName() + "!!");
            return;
        }
        
        File topDir = new File(prop.getProperty("coll"));
        indexDirectory(topDir);
    }

    private void indexDirectory(File dir) throws Exception {
        File[] files = dir.listFiles();
        for (int i=0; i < files.length; i++) {
            File f = files[i];
            if (f.isDirectory()) {
                System.out.println("Indexing directory " + f.getName());
                indexDirectory(f);  // recurse
            }
            else
                indexFile(f);
        }
    }
    
    void indexFile(File file) throws Exception {
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        String line;
        Document doc;

        System.out.println("Indexing file: " + file.getName());
        
        line = null;
        while ((line = br.readLine()) != null) {
            String[] row = line.split(";");
            doc = constructDoc(row);
            if (doc != null) {
                writer.addDocument(doc);
            }
        }
    }
    
    Document constructDoc(String[] row) throws Exception {
        Document doc = new Document();
        String docDomainName = row[0];
        String docTextElt = row[1];
        doc.add(new Field(WMTIndexer.FIELD_DOMAIN_ID, docDomainName, Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(NewsFields.FIELD_DOCNO, String.valueOf(docIdx), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(WMTIndexer.FIELD_URL, String.valueOf(docIdx++), Field.Store.YES, Field.Index.NOT_ANALYZED));
        
        System.out.println("Agrego nuevo doc ");
        System.out.println(docIdx - 1);
        
        String content = docTextElt;
        if (content.equals("null"))
            return null;
        
        doc.add(new Field(WMTIndexer.FIELD_ANALYZED_CONTENT, docTextElt,
                Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
        return doc;
    }
    
    
    public static void main(String[] args) {
        if (args.length == 0) {
            args = new String[1];
            System.out.println("Usage: java 20NewsMblogIndexer <prop-file>");
            args[0] = "20news.properties";
        }

        try {
            TwentyNewsIndexer indexer = new TwentyNewsIndexer("20news.properties");
            indexer.processAll();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }    
}
