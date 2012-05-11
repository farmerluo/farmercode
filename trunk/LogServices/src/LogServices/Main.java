/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package LogServices;
import org.apache.log4j.Logger;

/**
 *
 * @author FarmerLuo
 */
public class Main {

    public static Logger logger = Logger.getLogger(Main.class.getName());
    public static LoadData  load_data;
    public static CheckTimeout check_time;

    public static void main(String[] args) {

        org.apache.log4j.PropertyConfigurator.configure("./log4j.properties");
        logger.info( "LogServices start ..." );

        load_data = new LoadData();
        load_data.loadConfig();
        load_data.start();
        
        try {
            Thread.sleep(30000);
        } catch (InterruptedException ex) {
            logger.info( "ex." );
        }

        check_time = new CheckTimeout(load_data);
        check_time.start();

        //doShutDownWork();
   
    }

    private static void doShutDownWork() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public synchronized void run() {
                this.setName("doShutDownWork");
                logger.info("Waiting for exit LogServices ...");
                load_data.setflag(false);
                load_data.interrupt();
                logger.info( "LogServices exit done." );
            }
        });

    }


}
