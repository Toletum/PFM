package Pruebas;

import java.util.List;

import redis.clients.jedis.Jedis;

public class RedisExample {

    private static Jedis jedis;

	public static void main(String[] args) throws Exception {
		List<String>JCS;
		List<String>Pendientes=null;
		int iPendientes;
    	jedis = new Jedis("database");
    	
		jedis.del("JCS");
		
    	for(int i=1;true;i++) {
    		jedis.lpush("JCS", (i%10)+"");
    		jedis.ltrim("JCS", 0, 9);

    		JCS=jedis.lrange("JCS", 0, -1);
    		
    		for(int c=0;c<JCS.size();c++) {
    			System.out.print(JCS.get(c));
    			System.out.print(" ");
    		}
    		
			System.out.println();

			iPendientes=JCS.indexOf("2");
			
			System.out.print("POS: "+iPendientes+" --> ");
			
			if(iPendientes!=0) {
				if(iPendientes<0) {
	    			System.out.print(" TODOS ");
					Pendientes=jedis.lrange("JCS", 0, -1); //TODOS
				}
			
				if(iPendientes>0) {
					Pendientes=jedis.lrange("JCS", 0, iPendientes-1);
				}	
			
	    		for(int c=0;c<Pendientes.size();c++) {
	    			System.out.print(Pendientes.get(c));
	    			System.out.print(" ");
	    		}
	    		
			} else {
				System.out.print("NO HAY PENDIENTES");
			}
			System.out.println();
			
			
    		Thread.sleep(1000);
    	}
    }
	
}
