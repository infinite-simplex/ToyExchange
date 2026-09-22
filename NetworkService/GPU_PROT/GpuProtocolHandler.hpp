class GpuMatchingProtocolHandler {
public:
    size_t validateAndParse(const char* buf, size_t availableLen, GpuMatchingCommand& outCmd);
};