/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package des;

/**
 *
 * @author FarmerLuo
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            String desstr = DESCoder.desEncrypt("12354");
            String pstr = DESCoder.desDecrypt(desstr);
            System.out.println("plainText:12354");
            System.out.println("Encode:"+desstr);
            System.out.println("Decode:"+pstr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
