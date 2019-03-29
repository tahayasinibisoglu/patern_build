package org.apache.flink;

import java.util.ArrayList;
import java.util.List;

public class TreeNode{
    private String item;
    private TreeNode parentNode;
    private List<TreeNode> childNodes = new ArrayList<TreeNode>();
    private int counts;
    private TreeNode nextNode;


    public String getItem() {
        return item;
    }
    public void setItem(String item) {
        this.item = item;
    }
    public TreeNode getParentNode() {
        return parentNode;
    }
    public void setParentNode(TreeNode parentNode) {
        this.parentNode = parentNode;
    }
    public List<TreeNode> getChildNodes() {
        return childNodes;
    }
    public void setChildNodes(List<TreeNode> childNodes) {
        this.childNodes = childNodes;
    }
    public int getCounts() {
        return counts;
    }
    public void increCounts() {
        this.counts = counts + 1;
    }
    public TreeNode getNextNode() {
        return nextNode;
    }
    public void setNextNode(TreeNode nextNode) {
        this.nextNode = nextNode;
    }
    public void setCounts(int counts) {
        this.counts = counts;
    }
}
