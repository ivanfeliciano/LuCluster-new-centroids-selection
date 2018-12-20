/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clusterer;

import indexer.WMTIndexer;
import static indexer.WMTIndexer.FIELD_ANALYZED_CONTENT;
import static indexer.WMTIndexer.FIELD_DOMAIN_ID;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.BytesRef;

/**
 *
 * @author dganguly
 */
public class FPACDynamicCentroids extends LuceneClusterer {
    IndexSearcher searcher;
    RelatedDocumentsRetriever[] rdes;
    
    
    //Estos son los grupos de centroides
    ArrayList<ArrayList<RelatedDocumentsRetriever>> DynamicCentroids;
    RelatedDocumentsRetriever[][] CentroidsGroups;
    
    ArrayList<ArrayList<TermVector>> dynamicTermVectorCentroids;
    TermVector[][] termVectorCentroids;
    // Un conjunto de términos para cada cluster
    
    Set<String>[] listSetOfTermsForEachCluster;
    
    public FPACDynamicCentroids(String propFile) throws Exception {
        super(propFile);
        
        searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new BM25Similarity());        
        rdes = new RelatedDocumentsRetriever[K];
        listSetOfTermsForEachCluster = new HashSet[K];
        
        // Inicia estructura que guarda los centroides
        DynamicCentroids = new ArrayList<>();
        dynamicTermVectorCentroids = new ArrayList<>();
        CentroidsGroups = new RelatedDocumentsRetriever[K][numberOfCentroidsByGroup];
        termVectorCentroids =  new TermVector[K][numberOfCentroidsByGroup];
    }
    
    int selectDoc(HashSet<String> queryTerms) throws IOException {
        BooleanQuery.Builder b = new BooleanQuery.Builder();
        for (String qterm : queryTerms) {
            TermQuery tq = new TermQuery(new Term(contentFieldName, qterm));
            b.add(new BooleanClause(tq, BooleanClause.Occur.MUST_NOT));
        }
        
        TopDocsCollector collector = TopScoreDocCollector.create(1);
        searcher.search(b.build(), collector);
        TopDocs topDocs = collector.topDocs();
        return topDocs.scoreDocs == null || topDocs.scoreDocs.length == 0? -1 :
                topDocs.scoreDocs[0].doc;
    }
    
    // Initialize centroids
    // The idea is to select a random document. Grow a region around it and choose
    // as the next candidate centroid a document that does not belong to this region.
    // Same but with several centroids for each cluster
    // With the top list we select the most similar docs from an initial selected doc
    // at each iteration
    @Override
    void initCentroids() throws Exception {
        int selectedDoc = (int)(Math.random()*numDocs);
        int numClusterCentresAssigned = 1;
        centroidDocIds = new HashMap<>();
        for (int i = 0; i < K; i++) {
            DynamicCentroids.add(new ArrayList<> ());
            dynamicTermVectorCentroids.add(new ArrayList<>());
        }
        System.out.println("El número de clusters es " + DynamicCentroids.size());
        int idxCentroidsGroup;
        do {
            RelatedDocumentsRetriever rde = new RelatedDocumentsRetriever(reader, selectedDoc, prop, numClusterCentresAssigned);
            System.out.println("Chosen doc " + selectedDoc + " as first centroid for cluster " + numClusterCentresAssigned);
            TopDocs topDocs = rde.getRelatedDocs(numDocs/K);
            if (topDocs == null) {
                selectedDoc = rde.getUnrelatedDocument(centroidDocIds);
                continue;
            }
            centroidDocIds.put(selectedDoc, null);
            TermVector centroid = TermVector.extractAllDocTerms(reader, selectedDoc, contentFieldName, lambda);
            selectedDoc = rde.getUnrelatedDocument(centroidDocIds);
            CentroidsGroups[numClusterCentresAssigned-1][0] = rde;
            DynamicCentroids.get(numClusterCentresAssigned - 1).add(rde);
            dynamicTermVectorCentroids.get(numClusterCentresAssigned - 1).add(centroid);
            termVectorCentroids[numClusterCentresAssigned-1][0] = centroid;
            numClusterCentresAssigned++;
        } while (numClusterCentresAssigned <= K);
        
        numClusterCentresAssigned = 1;
        
        for (int clusterIdx = 0; clusterIdx < K; clusterIdx++) {
            RelatedDocumentsRetriever rde = CentroidsGroups[clusterIdx][0];
            TopDocs topDocs = rde.getRelatedDocs(numDocs / K);
            if (topDocs == null || topDocs.scoreDocs.length < numberOfCentroidsByGroup) {
                System.out.println("No pude encontrar doc relacionados D:");
                break;
            }
            idxCentroidsGroup = 1;
            for (int i = 1; i < numberOfCentroidsByGroup; i++) {
                ScoreDoc docFromTopDocs = topDocs.scoreDocs[i];
                centroidDocIds.put(docFromTopDocs.doc, null);
                
                
                DynamicCentroids.get(clusterIdx).add(new RelatedDocumentsRetriever(reader, docFromTopDocs.doc, prop, numClusterCentresAssigned));
                dynamicTermVectorCentroids.get(clusterIdx).add(TermVector.extractAllDocTerms(reader, docFromTopDocs.doc, contentFieldName, lambda));
                
                /////////////////////////
//                CentroidsGroups[clusterIdx][idxCentroidsGroup] = new RelatedDocumentsRetriever(reader, docFromTopDocs.doc, prop, numClusterCentresAssigned);
//                termVectorCentroids[clusterIdx][idxCentroidsGroup] = TermVector.extractAllDocTerms(reader, docFromTopDocs.doc, contentFieldName, lambda);
                ////////////////////////
                DynamicCentroids.get(clusterIdx).get(idxCentroidsGroup++).getRelatedDocs(numDocs / K); 
                
                
                ///////////////////////
//                CentroidsGroups[clusterIdx][idxCentroidsGroup++].getRelatedDocs(numDocs / K);
            }
        }
    }
    
    @Override
    void showCentroids() throws Exception {
        for (int i = 0, j = 1; i < K; i++) {
            System.out.println("Cluster " +  (i + 1) + " has the centroids:");
            j = 1;
            for (RelatedDocumentsRetriever rde: DynamicCentroids.get(i)) {
                Document doc = rde.queryDoc;
                System.out.println("Centroid " + ((j++) % (numberOfCentroidsByGroup + 1)) + ": " + doc.get(WMTIndexer.FIELD_DOMAIN_ID) + ", " + doc.get(idFieldName));
            }
        }
    }
    
    @Override
    boolean isCentroid(int docId) {
        for (int i=0; i < K; i++) {
            for (RelatedDocumentsRetriever rde : DynamicCentroids.get(i)) {
                if (rde.docId == docId)
                    return true;
            }
        }
        return false;
    }
    
    @Override
    int getClosestCluster(int docId) throws Exception { // O(K) computation...
        float maxScore = 0;
        int clusterId = 0;
        float localScore = 0;
        for (int i=0; i < K; i++) {
            localScore = 0;
            for (RelatedDocumentsRetriever rde : DynamicCentroids.get(i)) {
                if (rde.docScoreMap == null)
                    continue;
                ScoreDoc sd = rde.docScoreMap.get(docId);
                if (sd != null)
                    localScore += sd.score;
            }
            if (localScore > maxScore) {
                maxScore = localScore;
                clusterId = i;
            }
        }
        if (maxScore == 0) {
            // Retrieved in none... Assign to a random cluster id
            //clusterId = (int)(Math.random()*K);
            //System.out.println("Asigno aleatoriamente al doc " + docId + " al cluster " + clusterId);
            clusterId = getClosestClusterNotAssignedDoc(docId);
            
        }
        return clusterId;
    }
    int getClosestClusterNotAssignedDoc(int docId) throws Exception {
        TermVector docVec = TermVector.extractAllDocTerms(reader, docId, contentFieldName, lambda);
        if (docVec == null) {
            System.out.println("Skipping cluster assignment for empty doc, because the docs is empty: " + docId);
            numberOfDocsAssginedRandomly++;
            return (int)(Math.random()*K);
        }

        float maxSim = 0, sim = 0;
        int mostSimClusterId = 0;
        int clusterId = 0;
        for(int i = 0; i < K; i++)
            for (TermVector centroidVec : dynamicTermVectorCentroids.get(i)) {
                if (centroidVec == null) {
                    numberOfDocsAssginedRandomly++;
                    System.out.println("Skipping cluster assignment for empty doc because there is an empty centroid: " + docId);
                    return (int)(Math.random()*K);            
                }
                clusterId = i;
                sim = docVec.cosineSim(centroidVec);
                if (sim > maxSim) {
                    maxSim = sim;
                    mostSimClusterId = clusterId;
                }
            }
        
        return mostSimClusterId;
    }
    // Returns true if the cluster id is changed...
    @Override
    boolean assignClusterId(int docId, int clusterId) throws Exception {
//        for (int i = 0; i < DynamicCentroids.get(clusterId).size(); i++)
//            DynamicCentroids.get(clusterId).get(i).addDocId(docId);
//            CentroidsGroups[clusterId][i].addDocId(docId);}
        return super.assignClusterId(docId, clusterId);
    }
        
    
    ArrayList<ArrayList<Integer>> ListOfDocsForEachCluster() throws Exception {
        ArrayList<ArrayList<Integer>> docsIdForThisCluster = new ArrayList<>(K);
        for (int i = 0; i < numDocs; i++)
            docsIdForThisCluster.get(getClusterId(i)).add(i);
        return docsIdForThisCluster;
    }
    
    @Override
    void recomputeCentroids() throws Exception {
        System.out.println("Recalculando centroides");
        ArrayList<HashSet<String>> clustersVocabulary = new ArrayList<>();
        ArrayList<ArrayList<Integer>> docsInEachCluster = new ArrayList<>(K);
        
        int clusterId;
        Terms tfvector;
        TermsEnum termsEnum;
        BytesRef term;
        
        //Por cada Cluster
        for (int i = 0; i < K; i++) {
            clustersVocabulary.add(new HashSet<>());
            docsInEachCluster.add(new ArrayList<>());
            DynamicCentroids.get(i).clear();
            dynamicTermVectorCentroids.get(i).clear();
        }
        
        // Por cada documento
        for (int docId = 0; docId < numDocs; docId++) {
            clusterId = getClusterId(docId);
            docsInEachCluster.get(getClusterId(docId)).add(docId);
            tfvector = reader.getTermVector(docId, contentFieldName);
            if (tfvector == null || tfvector.size() == 0)
                continue;
            termsEnum = tfvector.iterator();
            while ((term = termsEnum.next()) != null) { // explore the terms for this field
                clustersVocabulary.get(clusterId).add(term.utf8ToString());
            }
        }
        
        // Por cada cluster
        for (int cluster = 0; cluster < K; cluster++) {
            HashSet<String> clusterVocabulary = clustersVocabulary.get(cluster);
            int idx = 0;
            Set<String> intersection;
            intersection = new HashSet<>();
            Set<String> bestDoc = new HashSet<>();
            int maxCover = 0;
            int bestDocId = 0;
            Boolean hasBeenSelected[] = new Boolean[docsInEachCluster.get(cluster).size()];
            while (!clusterVocabulary.isEmpty()) {
                maxCover = 0;
                // Por cada documento en este cluster
                for (int clusterDocsIdx = 0; clusterDocsIdx < docsInEachCluster.get(cluster).size(); clusterDocsIdx++) {
                    Set<String> docVocabulary = new HashSet<>();
                    int docId = docsInEachCluster.get(cluster).get(clusterDocsIdx);
                    tfvector = reader.getTermVector(docId, contentFieldName);
                    if (tfvector == null || tfvector.size() == 0)
                        continue;
                    termsEnum = tfvector.iterator();
                    while ((term = termsEnum.next()) != null) { // explore the terms for this field
                        docVocabulary.add(term.utf8ToString());
                    }
                    intersection = new HashSet<>(docVocabulary); // use the copy constructor
                    intersection.retainAll(clusterVocabulary);
                    if (intersection.size() > maxCover) {
                        maxCover = intersection.size();
                        bestDoc = intersection;
                        bestDocId = docId;
                        hasBeenSelected[clusterDocsIdx] = true;
                    }
                }
                if (maxCover == 0) { System.out.println("No cubrí el vocabulario pero ya no había documentos que cumplieran la propiedad"); break;}
                clusterVocabulary.removeAll(bestDoc);
                DynamicCentroids.get(cluster).add(new RelatedDocumentsRetriever(reader, bestDocId, prop, cluster + 1));
//                CentroidsGroups[cluster][idx] = new RelatedDocumentsRetriever(reader, bestDocId, prop, cluster + 1);
                dynamicTermVectorCentroids.get(cluster).add(TermVector.extractAllDocTerms(reader, bestDocId, contentFieldName, lambda));
//                termVectorCentroids[cluster][idx] = TermVector.extractAllDocTerms(reader, bestDocId, contentFieldName, lambda);
                
                DynamicCentroids.get(cluster).get(idx++).getRelatedDocs(numDocs / K);
//                System.out.println("El cluster " +  cluster + " tiene " + idx + " centroides");
                 
//                CentroidsGroups[cluster][idx++].getRelatedDocs(numDocs / K);
                
            }
            if (clusterVocabulary.isEmpty()) { System.out.println("Cubrí el vocabulario con " + idx + " centroides"); }
        }
        
    }
    
    public static void main(String[] args) {
        float changeRatio = 0;
        PrintStream out;
//        try {
//            out = new PrintStream(new FileOutputStream("logscFPACMCentroidsCheckClusterCoverage.txt"));
//            System.setOut(out);
//        } catch (FileNotFoundException ex) {
//            Logger.getLogger(FPACWithMCentroids.class.getName()).log(Level.SEVERE, null, ex);
//        }
        if (args.length == 0) {
            args = new String[1];
            System.out.println("Usage: java FastKMedoidsClusterer <prop-file>");
            args[0] = "init_0.properties";
        }
        
        try {
            LuceneClusterer fkmc = new FPACDynamicCentroids(args[0]);
            fkmc.cluster();
            boolean eval = Boolean.parseBoolean(fkmc.getProperties().getProperty("eval", "true"));
            if (eval) {
                ClusterEvaluator ceval = new ClusterEvaluator(args[0]);
                System.out.println("Purity: " + ceval.computePurity());
                System.out.println("NMI: " + ceval.computeNMI());            
                System.out.println("RI: " + ceval.computeRandIndex());            
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
