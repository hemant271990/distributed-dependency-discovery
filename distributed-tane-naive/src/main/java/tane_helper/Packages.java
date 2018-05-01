package tane_helper;

import java.io.Serializable;
import java.lang.String;
import java.util.*;

import tane_helper.Package;

public final class Packages implements Serializable {
    public List<Package> l_list;
    public List<Package> s_list;
    public List<Package> r_list;

    public Packages(){
        l_list = new ArrayList<Package>();
        s_list = new ArrayList<Package>();
        r_list = new ArrayList<Package>();
    }
}
