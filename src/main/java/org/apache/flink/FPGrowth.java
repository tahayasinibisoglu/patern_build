package org.apache.flink;

import java.io.IOException;
import java.util.*;

public class FPGrowth {
    private static final int MIN_SUPPORT = 0;

    public void itemSort(final Map<String, Integer> itemMap, ArrayList<ArrayList<String>> imtemSet) {

        for (List<String> items : imtemSet) {
            Collections.sort(items);
        }

        for(ArrayList<String> items : imtemSet) {
            Collections.sort(items, new Comparator<String>() {
                @Override
                public int compare(String key1, String key2) {
                    return itemMap.get(key2) - itemMap.get(key1);
                }
            });
        }
    }

    public ArrayList<TreeNode> buildHeadTable(ArrayList<ArrayList<String>> imtemSet) {
        ArrayList<TreeNode> head = new ArrayList<TreeNode>();

        Map<String, Integer> itemMap = new HashMap<String, Integer>();
        for(ArrayList<String> items : imtemSet) {
            for(String item : items) {
                if(itemMap.get(item) == null) {
                    itemMap.put(item, 1);
                } else {
                    itemMap.put(item, itemMap.get(item) + 1);
                }
            }
        }

        Iterator<String> ite = itemMap.keySet().iterator();
        String key;
       /*while(ite.hasNext()) {
            key = (String) ite.next();
            System.out.print(key + ":");
            System.out.println(itemMap.get(key));
        }*/
        List<String> abandonSet = new ArrayList<String>();

        while(ite.hasNext()) {
            key = (String)ite.next();
            if(itemMap.get(key) <MIN_SUPPORT) {
                ite.remove();
                abandonSet.add(key);
            } else {
                TreeNode tn = new TreeNode();
                tn.increCounts();
                tn.setItem(key);
                tn.setCounts(itemMap.get(key));
                head.add(tn);
            }
        }

        for(ArrayList<String> items : imtemSet) {
            items.removeAll(abandonSet);
        }

        itemSort(itemMap, imtemSet);

        Collections.sort(head, new Comparator<TreeNode>() {
            @Override
            public int compare(TreeNode key1, TreeNode key2) {
                return key2.getCounts() - key1.getCounts();
            }
        });
        return head;
    }

    public TreeNode findChildNode(String item, TreeNode curNode) {
        List<TreeNode> childs = curNode.getChildNodes();
        if(null != childs) {
            for(TreeNode tn : curNode.getChildNodes()) {
                if(tn.getItem().equals(item)) {
                    return tn;
                }
            }
        }
        return null;
    }


    public void addAdjNode(TreeNode tn, ArrayList<TreeNode> head) {
        TreeNode curNode = null;
        for(TreeNode node : head) {
            if(node.getItem().equals(tn.getItem())) {
                curNode = node;
                while(null != curNode.getNextNode()) {
                    curNode = curNode.getNextNode();
                }
                curNode.setNextNode(tn);
            }
        }
    }

    public TreeNode buildFPTree(ArrayList<ArrayList<String>> itemSet, ArrayList<TreeNode> head) {
        TreeNode root = new TreeNode();
        TreeNode curNode = root;

        for(ArrayList<String> items : itemSet) {
            for(String item : items) {
                TreeNode tmp = findChildNode(item, curNode);
                if(null == tmp) {
                    tmp = new TreeNode();
                    tmp.setItem(item);
                    tmp.setParentNode(curNode);
                    curNode.getChildNodes().add(tmp);
                    addAdjNode(tmp, head);
                }
                curNode = tmp;
                tmp.increCounts();
            }
            curNode = root;
        }
        return root;
    }

    public void FPAlgo(ArrayList<ArrayList<String>> itemSet, ArrayList<String> candidatePattern) {
        // build head table
        ArrayList<TreeNode> head = buildHeadTable(itemSet);

        // build FP tree
        TreeNode root = buildFPTree(itemSet, head);
        // recursion exit
        if(root.getChildNodes().size() == 0) {
            return;
        }

        // print pattern
        if(null != candidatePattern) {
            for(TreeNode tn : head) {
                for(String s : candidatePattern) {
                    System.out.print(s + " ");
                }
                System.out.println(tn.getItem() + ":" + tn.getCounts());
            }
        }

        for(TreeNode hd : head) {
            ArrayList<String> pattern = new ArrayList<String>();
            pattern.add(hd.getItem());
            if(null != candidatePattern) {
                pattern.addAll(candidatePattern);
            }
            // find conditional pattern base
            ArrayList<ArrayList<String>> newItemSet = new ArrayList<ArrayList<String>>();
            TreeNode curNode = hd.getNextNode();
            while (curNode != null) {
                int counter = curNode.getCounts();
                ArrayList<String> parentNodes = new ArrayList<String>();
                TreeNode parent = curNode;

                // traverse all parent nodes of curNode and put them into parentNodes
                while ((parent = parent.getParentNode()).getItem() != null) {
                    parentNodes.add(parent.getItem());
                }
                while (counter-- > 0) {
                    newItemSet.add(parentNodes);
                }
                curNode = curNode.getNextNode();
            }

            // recursive process
            FPAlgo(newItemSet, pattern);

            while(null != curNode) {
            }
        }
    }

    public ArrayList<ArrayList<String>> readFile(ArrayList<String> list) throws IOException {

        ArrayList<ArrayList<String>> dataSet = new ArrayList<ArrayList<String>>();

        for (String s : list)
        {
            String[] duplicate_string = s.split(",");
            ArrayList<String> duplicate_remove = new ArrayList<String>();
            for(int i=0; i<duplicate_string.length;i++){
                duplicate_remove.add(duplicate_string[i]);
            }
            ArrayList<String> newList = removeDuplicates(duplicate_remove);
            dataSet.add(newList);
        }
        return dataSet;
    }

    public static <T> ArrayList<T> removeDuplicates(ArrayList<T> list)
    {
        // Create a new ArrayList
        ArrayList<T> newList = new ArrayList<T>();

        // Traverse through the first list
        for (T element : list) {

            // If this element is not present in newList
            // then add it
            if (!newList.contains(element)) {

                newList.add(element);
            }
        }

        // return the new list
        return newList;
    }
}
