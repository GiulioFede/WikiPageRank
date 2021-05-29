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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class Node implements Writable,Comparable<Node> {

    private String outlinks;
    private double pageRank;
    private double pageRankReceived;

    public Node() {
        this.outlinks = "";
        this.pageRank = 0;
        this.pageRankReceived = -1;
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

        dataOutput.writeChars(outlinks);
        dataOutput.writeDouble(pageRank);
        dataOutput.writeDouble(pageRankReceived);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        outlinks = dataInput.readLine();
        pageRank = dataInput.readDouble();
        pageRankReceived = dataInput.readDouble();
    }
}
