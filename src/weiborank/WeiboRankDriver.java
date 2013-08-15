/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package weiborank;

public class WeiboRankDriver {
    
    public static void main(String[] args) throws Exception {
        int times = Integer.parseInt(args[2]);
        String[] forGB = {"", args[1]+"/Data0"};
        forGB[0] = args[0];
        GraphBuilder.main(forGB);
        String[] forItr = {"Data","Data"};
	for (int i = 0; i < times; i++) {
		forItr[0] = args[1] + "/Data" + (i);
		forItr[1] = args[1] + "/Data" + (i+1);
		WeiboRanklter.main(forItr);
	}
	String[] forRV = {args[1]+"/Data"+times, args[1]+"/FinalRank"};
	WeiboRankViewer.main(forRV);

    }
}
