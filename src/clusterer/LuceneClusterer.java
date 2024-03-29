/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clusterer;

import indexer.WMTIndexer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

/**
 *
 * @author Debasis
 */
public abstract class LuceneClusterer {
    Properties prop;
    IndexReader reader;  // the combined index to search
    int numDocs;
    int K;
    String contentFieldName;
    HashMap<Integer, Integer> clusterIdMap;
    String idFieldName;
    String refFieldName;
    HashMap<Integer, Byte> centroidDocIds;
    TermVector[] centroidVecs;
    float lambda;
    int numberOfCentroidsByGroup;
    int numberOfDocsAssginedRandomly = 0;

    public LuceneClusterer(String propFile) throws Exception {
        prop = new Properties();
        prop.load(new FileReader(propFile));                
        
        File indexDir = new File(prop.getProperty("index"));
        
        reader = DirectoryReader.open(FSDirectory.open(indexDir.toPath()));
        numDocs = reader.numDocs();
        System.out.println("Number of documents");
        System.out.println(numDocs);
        K = Integer.parseInt(prop.getProperty("numclusters", "200"));
        
        // Init el número de centroids por grupo
        numberOfCentroidsByGroup = Integer.parseInt(prop.getProperty("numberOfCentroidsByGroup", "5"));
        contentFieldName = prop.getProperty("content.field_name", WMTIndexer.FIELD_ANALYZED_CONTENT);
        idFieldName = prop.getProperty("id.field_name", WMTIndexer.FIELD_URL);        
        //refFieldName = prop.getProperty("ref.field_name", WMTIndexer.FIELD_DOMAIN_ID);
        refFieldName = prop.getProperty("ref.field_name", "none");
        if (refFieldName.equals("none"))
            refFieldName = null;
        
        clusterIdMap = new HashMap<>();
        centroidDocIds = new HashMap<>();
        lambda = Float.parseFloat(prop.getProperty("lm.termsel.lambda", "0.6f"));        
        centroidVecs = new TermVector[K];
    }
    
    abstract void initCentroids() throws Exception;
    abstract void recomputeCentroids() throws Exception;
    abstract boolean isCentroid(int docId);
    abstract int getClosestCluster(int docId) throws Exception;    
    abstract void showCentroids() throws Exception;
    
    public void cluster() throws Exception {        
        long gstart, gend, start, end;
        gstart = System.currentTimeMillis();
        resetAllClusterIds();
        initCentroids();
        int maxIters = Integer.parseInt(prop.getProperty("maxiters", "20"));
        float stopThreshold = Float.parseFloat(prop.getProperty("stopthreshold", "0.1"));
        float changeRatio;
        
        for (int i=1; i <= maxIters; i++) {
	    start = System.currentTimeMillis();

            System.out.println("Iteration : " + i);
            if (i ==  1 || i % 20 == 0) {
                showCentroids();
            }
            
            System.out.println("Reassigning cluster ids to non-centroid docs...");
            changeRatio = assignClusterIds();
            
            System.out.println(changeRatio + " fraction of the documents reassigned different clusters...");
            if (changeRatio < stopThreshold) {
                System.out.println("Stopping after " + i + " iterations...");
                break;
            }
            recomputeCentroids();
            System.out.println("numberOfDocsAssigendRandomly = " + numberOfDocsAssginedRandomly);
            numberOfDocsAssginedRandomly = 0;
            end = System.currentTimeMillis();
            System.out.println("Time to run till " + i + " iterations: " + (end-start)/1000 + " seconds");
            //saveClusterIds(i);
            if (i ==  1 || i % 50 == 0) {
                gend = System.currentTimeMillis();
                System.out.println("Global time until this iteration: " + (gend-gstart)/1000 + " seconds");
            }
        }
        gend = System.currentTimeMillis();
        System.out.println("Time to cluster: " + (gend-gstart)/1000 + " seconds");

        saveClusterIds(0);  // the global one
        reader.close();
    }
    
    void saveClusterIds(int iter) throws Exception {
        String fileName = prop.getProperty("cluster.idfile");
        if (iter > 0)
                fileName += "." + iter;
        FileWriter fw = new FileWriter(fileName);
        BufferedWriter bw = new BufferedWriter(fw);
        
        for (Map.Entry<Integer, Integer> e : clusterIdMap.entrySet()) {
            String docId = reader.document(e.getKey()).get(idFieldName);
            bw.write(e.getKey() + "\t" + e.getValue() + "\t" + docId + "\n");
        }
        
        bw.close();
        fw.close();
    }
    
    public Properties getProperties() { return prop; }
    
    int getClusterId(int docId) throws Exception {
        return clusterIdMap.get(docId);
    }
    
    boolean assignClusterId(int docId, int clusterId) throws Exception {

        int oldClusterId = clusterIdMap.get(docId);
        clusterIdMap.put(docId, clusterId);        
        return clusterId != oldClusterId;
    }
    
    // Call this before initializing the algorithm
    public void resetAllClusterIds() throws Exception {        
        for (int i=0; i < numDocs; i++) {
            clusterIdMap.put(i, 0);
        }        
    }
    
    // Assign cluster ids for non-centroid docs...
    // Normalize the ranked lists (in order to be able to compare against them)
    // For a document d that is not a centroid, let k = argmax_j sim(d, C_j) (computed after normalization)
    // Assign d to C_k.
    // Returns the ratio of #docs with changes in cluster id to the toal #docs
    float assignClusterIds() throws Exception {
        int numChanged = 0;
        
        for (int i=0; i < numDocs; i++) { // O(N.K)
            if (isCentroid(i))
                continue;
            
//            Document d = reader.document(i);            
//            String thisUrl = d.get(idFieldName);
            int clusterId = getClosestCluster(i);
            if (assignClusterId(i, clusterId))
                numChanged++;  // cluster id got changed
            
        }
        return numChanged/(float)numDocs;
    }
}
