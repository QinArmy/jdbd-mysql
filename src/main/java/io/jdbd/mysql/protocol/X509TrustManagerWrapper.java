package io.jdbd.mysql.protocol;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import java.security.InvalidAlgorithmParameterException;
import java.security.cert.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of X509TrustManager wrapping JVM X509TrustManagers to add expiration and identity check
 */
public final class X509TrustManagerWrapper implements X509TrustManager {

    private X509TrustManager origTm = null;
    private final boolean verifyServerCert;
    private final String hostName;
    private CertificateFactory certFactory = null;
    private PKIXParameters validatorParams = null;
    private CertPathValidator validator = null;

    public X509TrustManagerWrapper(X509TrustManager tm, boolean verifyServerCertificate, String hostName)
            throws CertificateException {
        this.origTm = tm;
        this.verifyServerCert = verifyServerCertificate;
        this.hostName = hostName;

        if (verifyServerCertificate) {
            try {
                Set<TrustAnchor> anch = Arrays.stream(tm.getAcceptedIssuers()).map(c -> new TrustAnchor(c, null)).collect(Collectors.toSet());
                this.validatorParams = new PKIXParameters(anch);
                this.validatorParams.setRevocationEnabled(false);
                this.validator = CertPathValidator.getInstance("PKIX");
                this.certFactory = CertificateFactory.getInstance("X.509");
            } catch (Exception e) {
                throw new CertificateException(e);
            }
        }

    }

    public X509TrustManagerWrapper(String hostName) {
        this.verifyServerCert = false;
        this.hostName = hostName;
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return this.origTm != null ? this.origTm.getAcceptedIssuers() : new X509Certificate[0];
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        for (X509Certificate x509Certificate : chain) {
            x509Certificate.checkValidity();
        }

        if (this.validatorParams != null) {
            X509CertSelector certSelect = new X509CertSelector();
            certSelect.setSerialNumber(chain[0].getSerialNumber());

            try {
                CertPath certPath = this.certFactory.generateCertPath(Arrays.asList(chain));
                // Validate against the truststore.
                CertPathValidatorResult result = this.validator.validate(certPath, this.validatorParams);
                // Check expiration for the CA used to validate this path.
                ((PKIXCertPathValidatorResult) result).getTrustAnchor().getTrustedCert().checkValidity();
            } catch (InvalidAlgorithmParameterException | CertPathValidatorException e) {
                throw new CertificateException(e);
            }
        }

        if (this.verifyServerCert) {
            if (this.origTm != null) {
                this.origTm.checkServerTrusted(chain, authType);
            } else {
                throw new CertificateException("Can't verify server certificate because no trust manager is found.");
            }

            // Validate server identity.
            if (this.hostName != null) {
                boolean hostNameVerified = false;

                // Check each one of the DNS-ID (or IP) entries from the certificate 'subjectAltName' field.
                // See https://tools.ietf.org/html/rfc6125#section-6.4 and https://tools.ietf.org/html/rfc2818#section-3.1
                final Collection<List<?>> subjectAltNames = chain[0].getSubjectAlternativeNames();
                if (subjectAltNames != null) {
                    boolean sanVerification = false;
                    for (final List<?> san : subjectAltNames) {
                        final Integer nameType = (Integer) san.get(0);
                        // dNSName   [2] IA5String
                        // iPAddress [7] OCTET STRING
                        if (nameType == 2) {
                            sanVerification = true;
                            if (verifyHostName((String) san.get(1))) {  // May contain a wildcard char.
                                // Host name is valid.
                                hostNameVerified = true;
                                break;
                            }
                        } else if (nameType == 7) {
                            sanVerification = true;
                            if (this.hostName.equalsIgnoreCase((String) san.get(1))) {
                                // Host name (IP) is valid.
                                hostNameVerified = true;
                                break;
                            }
                        }
                    }
                    if (sanVerification && !hostNameVerified) {
                        throw new CertificateException("Server identity verification failed. "
                                + "None of the DNS or IP Subject Alternative Name entries matched the server hostname/IP '" + this.hostName + "'.");
                    }
                }

                if (!hostNameVerified) {
                    // Fall-back to checking the Relative Distinguished Name CN-ID (Common Name/CN) from the certificate 'subject' field.
                    // https://tools.ietf.org/html/rfc6125#section-6.4.4
                    final String dn = chain[0].getSubjectX500Principal().getName(X500Principal.RFC2253);
                    String cn = null;
                    try {
                        LdapName ldapDN = new LdapName(dn);
                        for (Rdn rdn : ldapDN.getRdns()) {
                            if (rdn.getType().equalsIgnoreCase("CN")) {
                                cn = rdn.getValue().toString();
                                break;
                            }
                        }
                    } catch (InvalidNameException e) {
                        throw new CertificateException("Failed to retrieve the Common Name (CN) from the server certificate.");
                    }

                    if (!verifyHostName(cn)) {
                        throw new CertificateException(
                                "Server identity verification failed. The certificate Common Name '" + cn + "' does not match '" + this.hostName + "'.");
                    }
                }
            }
        }

        // Nothing else to validate.
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        this.origTm.checkClientTrusted(chain, authType);
    }

    /**
     * Verify the host name against the given pattern, using the rules specified in <a href="https://tools.ietf.org/html/rfc6125#section-6.4.3">RFC 6125,
     * Section 6.4.3</a>. Support wildcard character as defined in the RFC.
     *
     * @param ptn the pattern to match with the host name.
     * @return <code>true</code> if the host name matches the pattern, <code>false</code> otherwise.
     */
    private boolean verifyHostName(String ptn) {
        final int indexOfStar = ptn.indexOf('*');
        if (indexOfStar >= 0 && indexOfStar < ptn.indexOf('.')) {
            final String head = ptn.substring(0, indexOfStar);
            final String tail = ptn.substring(indexOfStar + 1);

            return this.hostName.regionMatches(true, 0, head, 0, head.length())
                    && this.hostName.regionMatches(true, this.hostName.length() - tail.length(), tail, 0, tail.length())
                    && this.hostName.substring(head.length(), this.hostName.length() - tail.length()).indexOf('.') < 0;
        }
        return this.hostName.equalsIgnoreCase(ptn);
    }

}
