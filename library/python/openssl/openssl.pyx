import os
import sys
import logging


cdef extern from "openssl/pem.h":
    ctypedef struct BIGNUM:
        pass

    ctypedef struct RSA:
        pass

    ctypedef struct BIO:
        pass

    ctypedef struct EVP_PKEY:
        pass

    ctypedef int pem_password_cb(char *buf, int size, int rwflag, void *userdata)

    ctypedef struct DSA:
        pass

    ctypedef struct DSA_SIG:
        pass

    BIO* BIO_new_file(const char *path, const char* path)
    int	BIO_free(BIO *a);
    EVP_PKEY *PEM_read_bio_PrivateKey(BIO*, EVP_PKEY**, pem_password_cb*, void*)
    int EVP_PKEY_free(EVP_PKEY*)
    int RSA_free(RSA*)
    int DSA_free(DSA*)
    int RSA_size(RSA*)
    const BIGNUM* RSA_get0_n(const RSA* d)
    const BIGNUM* RSA_get0_e(const RSA* d)
    const BIGNUM* DSA_get0_p(const DSA* d)
    const BIGNUM* DSA_get0_q(const DSA* d)
    const BIGNUM* DSA_get0_g(const DSA* d)
    const BIGNUM* DSA_get0_pub_key(const DSA* d)
    void DSA_SIG_get0(const DSA_SIG* sig, const BIGNUM** pr, const BIGNUM** ps)
    RSA* EVP_PKEY_get1_RSA(EVP_PKEY*)
    DSA* EVP_PKEY_get1_DSA(EVP_PKEY*)
    char* BN_bn2dec(BIGNUM*)
    int RSA_sign(int type, const unsigned char *m, unsigned int m_length, unsigned char *sigret, unsigned int *siglen, RSA *rsa)
    DSA_SIG* DSA_do_sign(const unsigned char *dgst, int dlen, DSA *dsa)
    int NID_sha1


cdef int null_cb(char *buf, int size, int rwflag, void *userdata):
    return 0


cdef class PrivateRsaKey:

    cdef RSA *_rsa

    def __cinit__(self, char *path, bint query_passwd=1):
        if not os.path.exists(path):
            raise ValueError("Path {} does not exist".format(path))

        cdef BIO *bio = NULL
        cdef EVP_PKEY *key = NULL
        cdef pem_password_cb *cb = NULL

        if not sys.stdin.isatty() or not sys.stdout.isatty() or not query_passwd:
            cb = null_cb

        try:
            bio = BIO_new_file(path, "r")
            if not bio:
                raise ValueError("Cannot load file")
            key = PEM_read_bio_PrivateKey(bio, NULL, cb, NULL)

            if not key:
                raise ValueError("Cannot read key")
            self._rsa = EVP_PKEY_get1_RSA(key)
            if not self._rsa:
                raise ValueError("Cannot get rsa data from key file")
        finally:
            if bio:
                BIO_free(bio)
            if key:
                EVP_PKEY_free(key)

    def __dealloc__(self):
        if self._rsa:
            RSA_free(self._rsa)

    def get_e(self):
        return BN_bn2dec(RSA_get0_e(self._rsa))

    def get_n(self):
        return BN_bn2dec(RSA_get0_n(self._rsa))

    def sign(self, digest):
        signature = b" " * RSA_size(self._rsa)
        cdef unsigned int signature_len = 0
        res = RSA_sign(NID_sha1, digest, len(digest), signature, &signature_len, self._rsa)
        if res == 1:
            return signature
        else:
            return None


cdef class PrivateDsaKey:

    cdef DSA* _dsa

    def __cinit__(self, char *path, bint query_passwd=1):
        if not os.path.exists(path):
            raise ValueError("Path {} does not exist".format(path))

        cdef BIO *bio = NULL
        cdef EVP_PKEY *key = NULL
        cdef pem_password_cb *cb = NULL

        if not sys.stdin.isatty() or not sys.stdout.isatty() or not query_passwd:
            cb = null_cb

        try:
            bio = BIO_new_file(path, "r")
            if not bio:
                raise ValueError("Cannot load file")

            key = PEM_read_bio_PrivateKey(bio, NULL, cb, NULL)
            if not key:
                raise ValueError("Cannot read key")

            self._dsa = EVP_PKEY_get1_DSA(key)
            if not self._dsa:
                raise ValueError("Cannot get dsa data from key file")
        finally:
            if bio:
                BIO_free(bio)
            if key:
                EVP_PKEY_free(key)

    def __dealloc__(self):
        if self._dsa:
            DSA_free(self._dsa)

    def get_p(self):
        return BN_bn2dec(DSA_get0_p(self._dsa))

    def get_q(self):
        return BN_bn2dec(DSA_get0_q(self._dsa))

    def get_g(self):
        return BN_bn2dec(DSA_get0_g(self._dsa))

    def get_y(self):
        return BN_bn2dec(DSA_get0_pub_key(self._dsa))

    def sign(self, digest):
        cdef const BIGNUM* r = NULL
        cdef const BIGNUM* s = NULL
        signature = DSA_do_sign(digest, len(digest), self._dsa)
        DSA_SIG_get0(signature, &r, &s)
        if signature:
            return BN_bn2dec(r), BN_bn2dec(s)
        return None

