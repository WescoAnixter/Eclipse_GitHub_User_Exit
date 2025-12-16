package com.informatica.mdm.hubuserexit.postload;

import java.util.Base64;

public class EnvironmentVariable {

	//public static String env_url = "https://wesco-prodint.mdm.informaticahosted.com:443/cmx/services/SifService";
    //public static String env_url = "https://wesco-sitint.mdm.informaticahostednp.com:443/cmx/services/SifService";
	public static String env_url = "https://wesco-devint.mdm.informaticahostednp.com:443/cmx/services/SifService";
	//public static String env_url = "https://wesco-qaint.mdm.informaticahosted.com:443/cmx/services/SifService";
		
	static String pswd = "ZGFpbHlSdW5AOTB0NQ=="; //DEV
	//static String pswd = "ZGFpbHlSdW5AODV5MQ=="; //SIT
	//static String pswd = "ZGFpbHlSdW5ANzZpMg=="; //QA
	//static String pswd = "ZGFpbHlSdW5AOTl0OA=="; //PROD
	
	static byte[] decrypt = Base64.getDecoder().decode(pswd);
    public static String env_pswd = new String(decrypt);
    
}
