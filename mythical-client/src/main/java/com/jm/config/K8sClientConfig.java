package com.jm.config;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/21 11:05
 */
@Component
public class K8sClientConfig {

    private KubernetesClient client;

    @PostConstruct
    public void init() {
        String url = "https://192.168.217.140:6443";
        String token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRyOVBjOGRFYU96N2N6c3VHTVo3dHV6X0J5eVR2SnktdnU5cm5FWU0ycVkifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJqaW5tdSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJqaW5tdS10b2tlbi05azhjeiIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJqaW5tdSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6IjFhZmExMjZmLTkxZDctNDhmNS04YTY5LWEyOGI1MmEzZjM2ZiIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpqaW5tdTpqaW5tdSJ9.m1dJdNU70ZS7XjZ3z9U8Vj554QOubSsqkN8QZEweJuIrwqQ-8D7Wd8HcepbRg_ByEVcwQ7NKcb01rCUnBDn8TaWGsQr56IrStXdWkQ8Z1dR0aMST8sh7ThaFFndng0pnHkQgEaF9X2ZxXTZjGWkuTRNjNq32Lh_PF4J0USpLTo7C-zPG_j9IGEvoR5lE9SSE7U1T4GhoiOlJiO4mdobCwuqkXEk3yUN9BSYuswz9rYghkNCu6HYRl15zyjO3UfdSL5FCkXW6cRq7Bc07HKwRIrtLrajFnH9EEyX0Kv4uZuuqRr6dO-gmyEtIzzrREmUbK_nubi_lXdHc8BAy9Tt7Cw";
        String cert = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUM1ekNDQWMrZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJek1EVXlOVEEzTkRZek5Wb1hEVE16TURVeU1qQTNORFl6TlZvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBTGVDClI4eUxPdVhKRmx4UWRyMWFXem5PNkJSQ3FUVTZSVmVlRXNRQlVJNVU1RnB3SVcxdTk2TGRGT2NrN2svSm1mQloKLyt4bFJRSDkrUjEvTGdtWUUzVkFmM3NNVUJGSk42b0E0NndVWmtjWnVnVnNaNVI5NXNUQ1o3Yk1lazNic2d0ZwpEUGZadzdMWnJmYURvNHhaRXVnRjBPNUtiOXdPQ1BmcnpSd2N2U1laUlFnMUJBU1ExYnJ3bi9sL2JuZmN3Mk1QCnNSUC9IS3dyWCtSSTFKYmZUdmdJV3prTzJ5WFJLSWsyamhEd1o2Z2dsZnZPRXBqS3UyQkpUWEh0L3pKZ3RITmYKZEE3WWZTQ0pNOExCT293K1NjRzBySFRlOHpFYTRKVHpjL2xzcVJnbjlaSDJ1VWIyNlZXOUJQTEJ5V0gycGxEVgpnL2xiVitEOGQyMm5YOXZJN0wwQ0F3RUFBYU5DTUVBd0RnWURWUjBQQVFIL0JBUURBZ0trTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0hRWURWUjBPQkJZRUZJMGtDVGN0QzVPb2FZQmJKNVh6cEJ6VGdsRlhNQTBHQ1NxR1NJYjMKRFFFQkN3VUFBNElCQVFBSkRCMHVFYmEvMTRmVFBjMi9taXE5TURmN0NZYUplUEUzWXZDMUpkZzV0UktKZWQ1dQo5ZGY5NWg0bXhmQTZLVFNzZU5JOFJrSzMrcGlDTnhvYjFHWXFuK3ZYWW1WOWhFdE9lcmhnd2k4YWdPVEN0eEl6CjRxK251R0I4amFtdU42bTV0WUt5YjZmQWNPM0ZtTWVLMXdrcU5sUHlYQTZCUzc5RVlSR0xpNEVNQ09VZEdIU3cKSU5WV3VneFBLYTRaNVFPZDZrWjZ6enFCQ0s2SlltN3NDbERwbmVQSDJETlRXZlJaNHJUZUlRdUNrbk5FTlRGMwpQMlVuaTFuQy9QdEtlOHJ4WEx4cE80WVloSDVhc0M0QUNuOTgvZWRDNG1JdEVtMjUvK0pkbHo3Ukh1WTZicURMCkFCdThxSFBoMXVxdjczeDl3c04rWUZBWjRhdTNFNUJRSFpJMQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==";

        io.fabric8.kubernetes.client.Config config = new ConfigBuilder()
                .withMasterUrl(url)
                .withOauthToken(token)
                .withCaCertData(cert)
                .build();

        this.client = new KubernetesClientBuilder()
                .withConfig(config)
                .build();

    }

    public KubernetesClient getClient() {
        return client;
    }
}
