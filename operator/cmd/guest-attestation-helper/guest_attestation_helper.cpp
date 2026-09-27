// guest_attestation_helper obtains a Microsoft Azure Attestation JWT from inside
// an Azure Confidential VM using Microsoft's azguestattestation client library.
//
// It prints only the JWT on stdout. Diagnostics go to stderr so the Go node
// agent can safely read stdout as the raw token.
#include <cstdlib>
#include <cstring>
#include <cstdarg>
#include <cstdio>
#include <iostream>
#include <string>

#include <azguestattestation1/AttestationClient.h>
#include <azguestattestation1/AttestationLibTypes.h>

namespace {

class StderrLogger final : public attest::AttestationLogger {
public:
    void Log(const char* log_tag,
             LogLevel level,
             const char* function,
             const int line,
             const char* fmt,
             ...) override {
        std::fprintf(stderr, "azguestattestation[%s] %s:%d %s: ",
                     LogLevelStrings[level].c_str(),
                     function == nullptr ? "unknown" : function,
                     line,
                     log_tag == nullptr ? "attestation" : log_tag);
        va_list args;
        va_start(args, fmt);
        std::vfprintf(stderr, fmt == nullptr ? "" : fmt, args);
        va_end(args);
        std::fprintf(stderr, "\n");
    }
};

std::string getenv_or(const char* key, const std::string& fallback) {
    const char* value = std::getenv(key);
    if (value == nullptr || std::strlen(value) == 0) {
        return fallback;
    }
    return std::string(value);
}

int error_code(attest::AttestationResult::ErrorCode code) {
    return static_cast<int>(code);
}

[[noreturn]] void finish(int code) {
    std::cout.flush();
    std::cerr.flush();
    std::_Exit(code);
}

}  // namespace

int main(int argc, char** argv) {
    const std::string endpoint = argc > 1 && std::strlen(argv[1]) > 0
                                     ? std::string(argv[1])
                                     : getenv_or("AZURE_ATTESTATION_ENDPOINT",
                                                 "https://sharedwus2.wus2.attest.azure.net");
    const std::string payload = argc > 2 && std::strlen(argv[2]) > 0
                                    ? std::string(argv[2])
                                    : getenv_or("AZURE_ATTESTATION_CLIENT_PAYLOAD", "{}");

    StderrLogger logger;
    AttestationClient* client = nullptr;
    if (!Initialize(&logger, &client) || client == nullptr) {
        std::cerr << "azguestattestation Initialize failed" << std::endl;
        finish(2);
    }

    attest::ClientParameters params{};
    params.version = CLIENT_PARAMS_VERSION;
    params.attestation_endpoint_url =
        reinterpret_cast<const unsigned char*>(endpoint.c_str());
    params.client_payload = reinterpret_cast<const unsigned char*>(payload.c_str());

    unsigned char* jwt_token = nullptr;
    attest::AttestationResult result = client->Attest(params, &jwt_token);
    if (result.code_ != attest::AttestationResult::ErrorCode::SUCCESS) {
        std::cerr << "azguestattestation Attest failed"
                  << " code=" << error_code(result.code_)
                  << " tpm_error_code=" << result.tpm_error_code_
                  << " description=" << result.description_ << std::endl;
        finish(10 + std::abs(error_code(result.code_)));
    }
    if (jwt_token == nullptr || std::strlen(reinterpret_cast<char*>(jwt_token)) == 0) {
        std::cerr << "azguestattestation returned empty JWT" << std::endl;
        finish(3);
    }

    std::cout << reinterpret_cast<char*>(jwt_token) << std::endl;
    finish(0);
}
