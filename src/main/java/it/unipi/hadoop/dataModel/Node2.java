package it.unipi.hadoop.dataModel;

public class Node2 extends Node {

    public Node2() {
        super();
    }

    @Override
    public String toString() {

        return String.valueOf(super.getPageRank());
    }
}
