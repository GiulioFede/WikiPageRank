package it.unipi.hadoop.dataModel;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class contains all the information of a page. In particular:
 * - outlinks -> list of the names of the other pages to which it refers
 * - pageRank -> page rank of the page
 * For optimization reasons this class also contains an additional field:
 * - pageRankReceived -> part of the page rank received from another page
 * This class will never see both 3 non-null fields, rather it can be in only two of the following states:
 * 1) outlinks and page rank not null (and therefore pageRankReceived null), therefore they only serve to identify the properties of the page
 * 2) null outlinks and page rank, therefore pageRankReceived! = Null will contain a fraction of the page rank of some other node / page
 **/
public class Node implements Writable,Comparable<Node>  {

    private String outlinks;
    private double pageRank;
    private double pageRankReceived;

    public Node() {
        this.outlinks = "";
        this.pageRank = -1;
        this.pageRankReceived = -1;
    }

    public Node(String outlinks, double pageRank, double pageRankReceived) {
        this.outlinks = outlinks;
        this.pageRank = pageRank;
        this.pageRankReceived = pageRankReceived;
    }

    public static Node copy(Node n){
        return new Node(n.getOutlinks(), n.getPageRank(), n.getPageRankReceived());
    }

    public String getOutlinks() {
        return outlinks;
    }

    public void setOutlinks(String outlinks) {
        this.outlinks = outlinks;
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public double getPageRankReceived() {
        return pageRankReceived;
    }

    public void setPageRankReceived(double pageRankReceived) {
        this.pageRankReceived = pageRankReceived;
    }
    
    public void reset(){
        this.outlinks = "";
        this.pageRank = -1;
        this.pageRankReceived = -1;
    }

    @Override
    public int compareTo(Node o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(outlinks);
        dataOutput.writeDouble(pageRank);
        dataOutput.writeDouble(pageRankReceived);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        outlinks = dataInput.readUTF();
        pageRank = dataInput.readDouble();
        pageRankReceived = dataInput.readDouble();
    }

    public static final String SEPARATOR = "//SEPARATOR//";
    @Override
    public String toString() {

        return  SEPARATOR + outlinks + SEPARATOR +
                pageRank +
                SEPARATOR + pageRankReceived + SEPARATOR;
    }
}
