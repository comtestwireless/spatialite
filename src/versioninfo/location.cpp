#ifndef CWI_API

#ifdef __cplusplus
# define CWI_C_API extern "C"
#else
# define CWI_C_API
#endif

#if defined _WIN32 || defined __CYGWIN__
# define CWI_HELPER_DLL_IMPORT __declspec(dllimport)
# define CWI_HELPER_DLL_EXPORT __declspec(dllexport)
# define CWI_HELPER_DLL_LOCAL
#else
# if __GNUC__ >= 4
#  define CWI_HELPER_DLL_IMPORT __attribute__ ((visibility ("default")))
#  define CWI_HELPER_DLL_EXPORT __attribute__ ((visibility ("default")))
#  define CWI_HELPER_DLL_LOCAL  __attribute__ ((visibility ("hidden")))
# else
#  define CWI_HELPER_DLL_IMPORT
#  define CWI_HELPER_DLL_EXPORT
#  define CWI_HELPER_DLL_LOCAL
# endif
#endif


#define CWI_API CWI_HELPER_DLL_EXPORT
#define CWI_LOCAL CWI_HELPER_DLL_LOCAL

#endif


#include <boost/dll/runtime_symbol_info.hpp>

#include <algorithm>
#include <cstdint>
#include <string>

CWI_C_API CWI_API int32_t get_location(char *buffer, uint32_t size) {
	try {
		auto path = boost::dll::symbol_location(get_location).string();
		if (size >= path.size()) {
			std::copy(path.begin(), path.end(), buffer);
		}

		return static_cast<int32_t>(path.size());
	} catch (...) {
		return -1;
	}
}
