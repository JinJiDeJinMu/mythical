package com.jm.mythical.k8s.config;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

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
        String token = "ZXlKaGJHY2lPaUpTVXpJMU5pSXNJbXRwWkNJNkluUnlPVkJqT0dSRllVOTZOMk42YzNWSFRWbzNkSFY2WDBKNWVWUjJTbmt0ZG5VNWNtNUZXVTB5Y1ZraWZRLmV5SnBjM01pT2lKcmRXSmxjbTVsZEdWekwzTmxjblpwWTJWaFkyTnZkVzUwSWl3aWEzVmlaWEp1WlhSbGN5NXBieTl6WlhKMmFXTmxZV05qYjNWdWRDOXVZVzFsYzNCaFkyVWlPaUpxYVc1dGRTSXNJbXQxWW1WeWJtVjBaWE11YVc4dmMyVnlkbWxqWldGalkyOTFiblF2YzJWamNtVjBMbTVoYldVaU9pSmhaRzFwYmkxMGIydGxiaTEwTWpKbWJDSXNJbXQxWW1WeWJtVjBaWE11YVc4dmMyVnlkbWxqWldGalkyOTFiblF2YzJWeWRtbGpaUzFoWTJOdmRXNTBMbTVoYldVaU9pSmhaRzFwYmlJc0ltdDFZbVZ5Ym1WMFpYTXVhVzh2YzJWeWRtbGpaV0ZqWTI5MWJuUXZjMlZ5ZG1salpTMWhZMk52ZFc1MExuVnBaQ0k2SWpoallXVTJObU5oTFdZelpXSXROREE1WVMxaU56RTVMVE0xT0RGaU5XSmhPR1F4WkNJc0luTjFZaUk2SW5ONWMzUmxiVHB6WlhKMmFXTmxZV05qYjNWdWREcHFhVzV0ZFRwaFpHMXBiaUo5LnNOMUVnZEdBMVBPRnBla1FucS1ENHg1dTE5dHhMbmVWNWZaZW9vbkNzVjM3V1RRUF8wUFUxMzlHSHk1b0dFQVdfRHRqOUgyejd4ZUJwQUYxdUdrLWoxYmxhOGUtSE53OC1DelpuMEJlUjFuTVZHbDNfcjFNak45c19DaGtVb25JT0hwMGMyUnNtSld3OEJsUTNHWHBheVFxazQ4QTVxR2ZWOUhhOEZXS1BSZjZ2eU5qclRxYVZCc2xDR2xlS1NycEZtQzZNb1NjbkhJZy03SDA5M3FaUlNJdHFIWTNyREdaRG5sZmRDbkYzaTBURXpMVS0zSW14LWdQcXlpWjRzblZBQVpJSXh0REtSWTJwMEZBdVg1R1lIcFlhd2dHMXBzWTkyUnZERE41Z1ZEVTU0X1dnZ2VkT2VUU0t5SURMWXhXRjVZSEJ0QU1mWVFCcHhxWFBVeW5Wdw==";
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

    @PreDestroy
    public void clean() {
        if (this.client != null) {
            this.client.close();
        }
    }

    public KubernetesClient getClient() {
        return client;
    }
}
