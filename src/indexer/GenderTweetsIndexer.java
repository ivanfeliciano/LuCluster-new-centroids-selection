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

interface GenderTweetFields {
    String FIELD_DOCNO = "docno";
    String FIELD_TIME = "time";
    String FIELD_USERNAME = "user";
}

public class GenderTweetsIndexer {
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
    
    public GenderTweetsIndexer(String propFile) throws Exception {
        prop = new Properties();
        prop.load(new FileReader(propFile));                
        analyzer = constructAnalyzer();            
        String indexPath = prop.getProperty("index");        
        indexDir = new File(indexPath);
    }
    
    public Analyzer getAnalyzer() { return analyzer; }

    void processAll() throws Exception {
        System.out.println("Indexing Twitter collection...");
        
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
        
        StringBuffer txtbuff = new StringBuffer();
        line = null;
        while ((line = br.readLine()) != null) {
            /*JSONParser parser =   new JSONParser();
            JSONObject json = (JSONObject) parser.parse(line);
            doc = constructDoc(json);
            if (doc != null) {
                writer.addDocument(doc);
            }*/
            String[] tweetFromCSV = line.split(",");
            doc = constructDoc(tweetFromCSV);
            if (doc != null) {
                writer.addDocument(doc);
            }
        }
        
           /* txtbuff.append(line).append("\n");
        String content = txtbuff.toString();
        org.jsoup.nodes.Document jdoc = Jsoup.parse(content);
        org.jsoup.select.Elements jdocBodyElement =  jdoc.select("body");
        */
        //
        
        //Elements docElts = jdoc.select("body");
        
        /*for (Element docElt : docElts) {
            doc = constructDoc(docElt);
            if (doc != null)
                writer.addDocument(doc);
        }*/
    }
    
    Document constructDoc(String[] tweetCSV) throws Exception {
        Document doc = new Document();
        String docDomainName = tweetCSV[0];
        String docTimeStringElt = tweetCSV[1];
        String docTimeElt = tweetCSV[2];
        String docUserElt = tweetCSV[4];
        String docTextElt = tweetCSV[5];
        doc.add(new Field(WMTIndexer.FIELD_DOMAIN_ID, docDomainName, Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(WMTIndexer.FIELD_URL, docTimeStringElt, Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(TweetFields.FIELD_DOCNO, String.valueOf(docIdx++), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(TweetFields.FIELD_TIME, docTimeElt, Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(TweetFields.FIELD_USERNAME, docUserElt, Field.Store.YES, Field.Index.NOT_ANALYZED));
        
        System.out.println("Agrego nuevo doc ");
        System.out.println(docIdx - 1);
        
        String content = docTextElt;
        if (content.equals("null"))
            return null;
        
        doc.add(new Field(WMTIndexer.FIELD_ANALYZED_CONTENT, docTextElt,
                Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
        return doc;
    }
    
    Document constructDoc(JSONObject docElt) throws Exception {
        
        //Object docIdElt = docIdx++;
        Object docTextElt = docElt.get("text");
        Object docTimeElt = docElt.get("created_at");
        Object docTimeStringElt = docElt.get("id_str");
        JSONObject JSONuser = (JSONObject) docElt.get("user");
        Object docUserElt;
        docUserElt = JSONuser.get("screen_name");
        
        Document doc = new Document();
        doc.add(new Field(WMTIndexer.FIELD_URL, docTimeStringElt.toString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(TweetFields.FIELD_DOCNO, String.valueOf(docIdx++), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(TweetFields.FIELD_TIME, docTimeElt.toString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(TweetFields.FIELD_USERNAME, docUserElt.toString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
        
        System.out.println("Agrego nuevo doc ");
        System.out.println(docIdx - 1);
        
        String content = docTextElt.toString();
        if (content.equals("null"))
            return null;
        
        doc.add(new Field(WMTIndexer.FIELD_ANALYZED_CONTENT, docTextElt.toString(),
                Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
        return doc;
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            args = new String[1];
            System.out.println("Usage: java TrecMblogIndexer <prop-file>");
            args[0] = "tweets.properties";
        }

        try {
            GenderTweetsIndexer indexer = new GenderTweetsIndexer("tweets.properties");
            indexer.processAll();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }    
}
