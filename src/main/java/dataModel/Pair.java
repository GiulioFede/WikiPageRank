package dataModel;

/*
    this class contains information about a page in a form of --> title, outlinks
 */

import java.util.ArrayList;

public class Pair {

    private String title;
    private ArrayList<String> outlinks;

    public Pair(String title, ArrayList<String> outlinks) {
        this.title = title;
        this.outlinks = outlinks;
    }

    public Pair() {

    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public ArrayList<String> getOutlinks() {
        return outlinks;
    }

    public void setOutlinks(ArrayList<String> outlinks) {
        this.outlinks = outlinks;
    }
}
