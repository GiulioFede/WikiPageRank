package it.unipi.hadoop.dataModel;

/*
    Questa classe contiene tutte le informazioni di una pagina. In particolare:
        - outlinks --> lista dei nomi delle altre pagine a cui si riferisce
        - pageRank --> page rank della pagina
    Per questioni di ottimizzazione tale classe contiene anche un ulteriore campo:
        - pageRankReceived -_> parte del page rank ricevuto da un altra pagina

    Tale classe non vedrà mai entrambi i 3 campi non nulli, piuttosto può trovarsi in solo due dei seguenti stati:
    1) outlinks e page rank non nulli (e quindi pageRankReceived nullo), pertanto servono unicamente a identificare le proprietà della pagina
    2) outlinks e page rank nulli, pertanto pageRankReceived != nullo conterrà una frazione del page rank di qualche altro nodo/pagina
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

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

    @Override
    public String toString() {

        return  outlinks +
                CustomPattern.SEPARATOR + pageRank +
                CustomPattern.SEPARATOR + pageRankReceived + CustomPattern.SEPARATOR;
    }
}
