/**
 * AnyDOIProvider
 *
 * This class is to implement a minimal DOI provider.
 * It will use a HTTP post endpoint to register a DOI.
 * And use a HTTP get endpoint to check if a DOI is already registered.
 *
 */

package edu.harvard.iq.dataverse.pidproviders.doi.any;

import edu.harvard.iq.dataverse.DvObject;
import edu.harvard.iq.dataverse.GlobalId;
import edu.harvard.iq.dataverse.pidproviders.doi.AbstractDOIProvider;

import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

// import necessary jakarta.json classes to dump hashmap to json
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;



public class AnyDOIProvider extends AbstractDOIProvider {

    private static final Logger logger = Logger.getLogger(AnyDOIProvider.class.getCanonicalName());

    public static final String TYPE = "anydoi";

    private String anyDoiUrl;
    private CloseableHttpClient httpClient;
    private HttpClientContext context;

    public AnyDOIProvider(String id,
                          String label,
                          String providerAuthority,
                          String providerShoulder,
                          String identifierGenerationStyle,
                          String datafilePidFormat,
                          String managedList,
                          String excludedList,
                          String anyDoiUrl) {
        super(id, label, providerAuthority, providerShoulder, identifierGenerationStyle, datafilePidFormat, managedList, excludedList);
        this.anyDoiUrl = anyDoiUrl;
        logger.info("AnyDOIProvider created with url: " + anyDoiUrl);
        context = HttpClientContext.create();
        httpClient = HttpClients.createDefault();
    }

    @Override
    public boolean alreadyRegistered(GlobalId pid, boolean noProviderDefault) {
        if (pid == null || pid.asString().isEmpty()) {
            return false;
        }
        boolean alreadyRegistered;
        String identifier = pid.asString();
        logger.info("Checking if identifier is already registered: " + identifier);
        try {
            // send a http get request to /doi/{identifier} to check if the identifier is already registered
            // return true if the status code is 404
            String encodedIdentifier = URLEncoder.encode(identifier, "UTF-8");
            logger.info("Encoded identifier: " + encodedIdentifier);
            HttpGet httpGet = new HttpGet(anyDoiUrl + "/doi/" + encodedIdentifier);
            HttpResponse response = httpClient.execute(httpGet, context);
            alreadyRegistered = response.getStatusLine().getStatusCode() != 404;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error checking if identifier is already registered", e);
            return false;
        }
        return alreadyRegistered;
    }

    @Override
    public boolean registerWhenPublished() {
        return true;
    }

    @Override
    public List<String> getProviderInformation() {
        return List.of(getId(), anyDoiUrl);
    }

    @Override
    public String createIdentifier(DvObject dvObject) throws Throwable {
        if (dvObject.getIdentifier() == null || dvObject.getIdentifier().isEmpty()) {
            dvObject = generatePid(dvObject);
        }
        String identifier = getIdentifier(dvObject);
        Map<String, String> metadata = getMetadataForCreateIndicator(dvObject);
        // TODO: support draft status
        try {
            // create json string from identifier, metadata
            HashMap<String, Object> requestData = new HashMap<>();
            requestData.put("identifier", identifier);
            requestData.put("metadata", metadata);
            Jsonb jsonb = JsonbBuilder.create();
            String body = jsonb.toJson(requestData);

            HttpPost httpPost = new HttpPost(anyDoiUrl + "/doi");
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setEntity(new StringEntity(body, "utf-8"));
            HttpResponse response = httpClient.execute(httpPost, context);
            String data = response.getEntity().toString();
            if (response.getStatusLine().getStatusCode() != 201) {
                String errMsg = "Response from createIdentifier: " + response.getStatusLine().getStatusCode() + ", " + data;
                throw new Exception(errMsg);
            }
            return "OK";
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public Map<String, String> getIdentifierMetadata(DvObject dvo) {
        Map<String, String> map = new HashMap<>();
        return map;
    }

    @Override
    public void deleteIdentifier(DvObject dvo) throws Exception {
        // no-op
    }

    @Override
    public boolean publicizeIdentifier(DvObject dvObject) {
        // TODO: anydoi should support draft
        return true;
    }

    @Override
    public String modifyIdentifierTargetURL(DvObject dvo) throws Exception {
        // TODO
        String identifier = getIdentifier(dvo);
        return identifier;
    }

    @Override
    protected String getProviderKeyName() {
        // TODO: this should be get from the / endpoint
        return "Chinese DOI";
    }

    @Override
    public String getProviderType() {
        return TYPE;
    }
}
