#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/work.hpp>
#include <nano/node/openclconfig.hpp>
#include <nano/node/openclwork.hpp>
#include <nano/node/wallet.hpp>

#include <boost/format.hpp>

#include <array>
#include <iostream>
#include <string>
#include <vector>

#if defined(__APPLE__)
bool nano::opencl_loaded{ true };
#else
bool nano::opencl_loaded{ false };
#endif

namespace
{
std::string opencl_program = R"%%%(
enum Blake2b_IV {
    iv0 = 0x6a09e667f3bcc908UL,
    iv1 = 0xbb67ae8584caa73bUL,
    iv2 = 0x3c6ef372fe94f82bUL,
    iv3 = 0xa54ff53a5f1d36f1UL,
    iv4 = 0x510e527fade682d1UL,
    iv5 = 0x9b05688c2b3e6c1fUL,
    iv6 = 0x1f83d9abfb41bd6bUL,
    iv7 = 0x5be0cd19137e2179UL,
};

enum IV_Derived {
    nano_xor_iv0 = 0x6a09e667f2bdc900UL,  // iv1 ^ 0x1010000 ^ outlen
    nano_xor_iv4 = 0x510e527fade682f9UL,  // iv4 ^ inbytes
    nano_xor_iv6 = 0xe07c265404be4294UL,  // iv6 ^ ~0
};

#ifdef cl_amd_media_ops
#pragma OPENCL EXTENSION cl_amd_media_ops : enable
static inline ulong rotr64(ulong x, int shift)
{
    uint2 x2 = as_uint2(x);
    if (shift < 32)
        return as_ulong(amd_bitalign(x2.s10, x2, shift));
    return as_ulong(amd_bitalign(x2, x2.s10, (shift - 32)));
}
#else
static inline ulong rotr64(ulong x, int shift)
{
    return rotate(x, 64UL - shift);
}
#endif

#define G32(m0, m1, m2, m3, vva, vb1, vb2, vvc, vd1, vd2) \
    do {                                                  \
        vva += (ulong2)(vb1 + m0, vb2 + m2);              \
        vd1 = rotr64(vd1 ^ vva.s0, 32);                   \
        vd2 = rotr64(vd2 ^ vva.s1, 32);                   \
        vvc += (ulong2)(vd1, vd2);                        \
        vb1 = rotr64(vb1 ^ vvc.s0, 24);                   \
        vb2 = rotr64(vb2 ^ vvc.s1, 24);                   \
        vva += (ulong2)(vb1 + m1, vb2 + m3);              \
        vd1 = rotr64(vd1 ^ vva.s0, 16);                   \
        vd2 = rotr64(vd2 ^ vva.s1, 16);                   \
        vvc += (ulong2)(vd1, vd2);                        \
        vb1 = rotr64(vb1 ^ vvc.s0, 63);                   \
        vb2 = rotr64(vb2 ^ vvc.s1, 63);                   \
    } while (0)

#define G2v(m0, m1, m2, m3, a, b, c, d)                                   \
    G32(m0, m1, m2, m3, vv[a / 2], vv[b / 2].s0, vv[b / 2].s1, vv[c / 2], \
        vv[d / 2].s0, vv[d / 2].s1)

#define G2v_split(m0, m1, m2, m3, a, vb1, vb2, c, vd1, vd2) \
    G32(m0, m1, m2, m3, vv[a / 2], vb1, vb2, vv[c / 2], vd1, vd2)

#define ROUND(m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, \
              m15)                                                             \
    do {                                                                       \
        G2v(m0, m1, m2, m3, 0, 4, 8, 12);                                      \
        G2v(m4, m5, m6, m7, 2, 6, 10, 14);                                     \
        G2v_split(m8, m9, m10, m11, 0, vv[5 / 2].s1, vv[6 / 2].s0, 10,         \
                  vv[15 / 2].s1, vv[12 / 2].s0);                               \
        G2v_split(m12, m13, m14, m15, 2, vv[7 / 2].s1, vv[4 / 2].s0, 8,        \
                  vv[13 / 2].s1, vv[14 / 2].s0);                               \
    } while (0)

static inline ulong blake2b(ulong const nonce, __constant ulong *h)
{
    ulong2 vv[8] = {
        {nano_xor_iv0, iv1}, {iv2, iv3},          {iv4, iv5},
        {iv6, iv7},          {iv0, iv1},          {iv2, iv3},
        {nano_xor_iv4, iv5}, {nano_xor_iv6, iv7},
    };

    ROUND(nonce, h[0], h[1], h[2], h[3], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    ROUND(0, 0, h[3], 0, 0, 0, 0, 0, h[0], 0, nonce, h[1], 0, 0, 0, h[2]);
    ROUND(0, 0, 0, nonce, 0, h[1], 0, 0, 0, 0, h[2], 0, 0, h[0], 0, h[3]);
    ROUND(0, 0, h[2], h[0], 0, 0, 0, 0, h[1], 0, 0, 0, h[3], nonce, 0, 0);
    ROUND(0, nonce, 0, 0, h[1], h[3], 0, 0, 0, h[0], 0, 0, 0, 0, h[2], 0);
    ROUND(h[1], 0, 0, 0, nonce, 0, 0, h[2], h[3], 0, 0, 0, 0, 0, h[0], 0);
    ROUND(0, 0, h[0], 0, 0, 0, h[3], 0, nonce, 0, 0, h[2], 0, h[1], 0, 0);
    ROUND(0, 0, 0, 0, 0, h[0], h[2], 0, 0, nonce, 0, h[3], 0, 0, h[1], 0);
    ROUND(0, 0, 0, 0, 0, h[2], nonce, 0, 0, h[1], 0, 0, h[0], h[3], 0, 0);
    ROUND(0, h[1], 0, h[3], 0, 0, h[0], 0, 0, 0, 0, 0, h[2], 0, 0, nonce);
    ROUND(nonce, h[0], h[1], h[2], h[3], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    ROUND(0, 0, h[3], 0, 0, 0, 0, 0, h[0], 0, nonce, h[1], 0, 0, 0, h[2]);

    return nano_xor_iv0 ^ vv[0].s0 ^ vv[4].s0;
}
#undef G32
#undef G2v
#undef G2v_split
#undef ROUND

__kernel void nano_work(__constant ulong *attempt,
                        __global ulong *result_a,
                        __constant uchar *item_a,
                        __constant ulong *difficulty)
{
    ulong const attempt_l = *attempt + get_global_id(0);
    if (blake2b(attempt_l, item_a) >= *difficulty)
        *result_a = attempt_l;
}
)%%%";
}

nano::opencl_environment::opencl_environment (bool & error_a)
{
	if (nano::opencl_loaded)
	{
		cl_uint platformIdCount = 0;
		clGetPlatformIDs (0, nullptr, &platformIdCount);
		std::vector<cl_platform_id> platformIds (platformIdCount);
		clGetPlatformIDs (platformIdCount, platformIds.data (), nullptr);
		for (auto i (platformIds.begin ()), n (platformIds.end ()); i != n; ++i)
		{
			nano::opencl_platform platform;
			platform.platform = *i;
			cl_uint deviceIdCount = 0;
			clGetDeviceIDs (*i, CL_DEVICE_TYPE_ALL, 0, nullptr, &deviceIdCount);
			std::vector<cl_device_id> deviceIds (deviceIdCount);
			clGetDeviceIDs (*i, CL_DEVICE_TYPE_ALL, deviceIdCount, deviceIds.data (), nullptr);
			for (auto j (deviceIds.begin ()), m (deviceIds.end ()); j != m; ++j)
			{
				platform.devices.push_back (*j);
			}
			platforms.push_back (platform);
		}
	}
	else
	{
		error_a = true;
	}
}

void nano::opencl_environment::dump (std::ostream & stream)
{
	if (nano::opencl_loaded)
	{
		auto index (0);
		size_t device_count (0);
		for (auto & i : platforms)
		{
			device_count += i.devices.size ();
		}
		stream << boost::str (boost::format ("OpenCL found %1% platforms and %2% devices\n") % platforms.size () % device_count);
		for (auto i (platforms.begin ()), n (platforms.end ()); i != n; ++i, ++index)
		{
			std::vector<unsigned> queries = { CL_PLATFORM_PROFILE, CL_PLATFORM_VERSION, CL_PLATFORM_NAME, CL_PLATFORM_VENDOR, CL_PLATFORM_EXTENSIONS };
			stream << "Platform: " << index << std::endl;
			for (auto j (queries.begin ()), m (queries.end ()); j != m; ++j)
			{
				size_t platformInfoCount = 0;
				clGetPlatformInfo (i->platform, *j, 0, nullptr, &platformInfoCount);
				std::vector<char> info (platformInfoCount);
				clGetPlatformInfo (i->platform, *j, info.size (), info.data (), nullptr);
				stream << info.data () << std::endl;
			}
			for (auto j (i->devices.begin ()), m (i->devices.end ()); j != m; ++j)
			{
				std::vector<unsigned> queries = { CL_DEVICE_NAME, CL_DEVICE_VENDOR, CL_DEVICE_PROFILE };
				stream << "Device: " << j - i->devices.begin () << std::endl;
				for (auto k (queries.begin ()), o (queries.end ()); k != o; ++k)
				{
					size_t platformInfoCount = 0;
					clGetDeviceInfo (*j, *k, 0, nullptr, &platformInfoCount);
					std::vector<char> info (platformInfoCount);
					clGetDeviceInfo (*j, *k, info.size (), info.data (), nullptr);
					stream << '\t' << info.data () << std::endl;
				}
				size_t deviceTypeCount = 0;
				clGetDeviceInfo (*j, CL_DEVICE_TYPE, 0, nullptr, &deviceTypeCount);
				std::vector<uint8_t> deviceTypeInfo (deviceTypeCount);
				clGetDeviceInfo (*j, CL_DEVICE_TYPE, deviceTypeCount, deviceTypeInfo.data (), 0);
				std::string device_type_string;
				switch (deviceTypeInfo[0])
				{
					case CL_DEVICE_TYPE_ACCELERATOR:
						device_type_string = "ACCELERATOR";
						break;
					case CL_DEVICE_TYPE_CPU:
						device_type_string = "CPU";
						break;
					case CL_DEVICE_TYPE_CUSTOM:
						device_type_string = "CUSTOM";
						break;
					case CL_DEVICE_TYPE_DEFAULT:
						device_type_string = "DEFAULT";
						break;
					case CL_DEVICE_TYPE_GPU:
						device_type_string = "GPU";
						break;
					default:
						device_type_string = "Unknown";
						break;
				}
				stream << '\t' << device_type_string << std::endl;
				size_t compilerAvailableCount = 0;
				clGetDeviceInfo (*j, CL_DEVICE_COMPILER_AVAILABLE, 0, nullptr, &compilerAvailableCount);
				std::vector<uint8_t> compilerAvailableInfo (compilerAvailableCount);
				clGetDeviceInfo (*j, CL_DEVICE_COMPILER_AVAILABLE, compilerAvailableCount, compilerAvailableInfo.data (), 0);
				stream << "\tCompiler available: " << (compilerAvailableInfo[0] ? "true" : "false") << std::endl;
				size_t computeUnitsAvailableCount = 0;
				clGetDeviceInfo (*j, CL_DEVICE_MAX_COMPUTE_UNITS, 0, nullptr, &computeUnitsAvailableCount);
				std::vector<uint8_t> computeUnitsAvailableInfo (computeUnitsAvailableCount);
				clGetDeviceInfo (*j, CL_DEVICE_MAX_COMPUTE_UNITS, computeUnitsAvailableCount, computeUnitsAvailableInfo.data (), 0);
				uint64_t computeUnits (computeUnitsAvailableInfo[0] | (computeUnitsAvailableInfo[1] << 8) | (computeUnitsAvailableInfo[2] << 16) | (computeUnitsAvailableInfo[3] << 24));
				stream << "\tCompute units available: " << computeUnits << std::endl;
				cl_ulong size{ 0 };
				clGetDeviceInfo (*j, CL_DEVICE_MAX_CONSTANT_BUFFER_SIZE, sizeof (cl_ulong), &size, 0);
				stream << "\tMemory size" << std::endl;
				stream << "\t\tConstant buffer: " << size << std::endl;
				clGetDeviceInfo (*j, CL_DEVICE_LOCAL_MEM_SIZE, sizeof (cl_ulong), &size, 0);
				stream << "\t\tLocal memory   : " << size << std::endl;
				clGetDeviceInfo (*j, CL_DEVICE_GLOBAL_MEM_SIZE, sizeof (cl_ulong), &size, 0);
				stream << "\t\tGlobal memory  : " << size << std::endl;
				clGetDeviceInfo (*j, CL_DEVICE_GLOBAL_MEM_CACHE_SIZE, sizeof (cl_ulong), &size, 0);
				stream << "\t\tGlobal cache   : " << size << std::endl;
				clGetDeviceInfo (*j, CL_DEVICE_MAX_MEM_ALLOC_SIZE, sizeof (cl_ulong), &size, 0);
				stream << "\t\tMax allocation : " << size << std::endl;
			}
		}
	}
	else
	{
		stream << boost::str (boost::format ("OpenCL library could not be found\n"));
	}
}

nano::opencl_work::opencl_work (bool & error_a, nano::opencl_config const & config_a, nano::opencl_environment & environment_a, nano::logger_mt & logger_a) :
config (config_a),
context (0),
attempt_buffer (0),
result_buffer (0),
item_buffer (0),
difficulty_buffer (0),
program (0),
kernel (0),
queue (0),
logger (logger_a)
{
	error_a |= config.platform >= environment_a.platforms.size ();
	if (!error_a)
	{
		auto & platform (environment_a.platforms[config.platform]);
		error_a |= config.device >= platform.devices.size ();
		if (!error_a)
		{
			nano::random_pool::generate_block (reinterpret_cast<uint8_t *> (rand.s.data ()), rand.s.size () * sizeof (decltype (rand.s)::value_type));
			std::array<cl_device_id, 1> selected_devices;
			selected_devices[0] = platform.devices[config.device];
			cl_context_properties contextProperties[] = {
				CL_CONTEXT_PLATFORM,
				reinterpret_cast<cl_context_properties> (platform.platform),
				0, 0
			};
			cl_int createContextError (0);
			context = clCreateContext (contextProperties, static_cast<cl_uint> (selected_devices.size ()), selected_devices.data (), nullptr, nullptr, &createContextError);
			error_a |= createContextError != CL_SUCCESS;
			if (!error_a)
			{
				cl_int queue_error (0);
				queue = clCreateCommandQueue (context, selected_devices[0], 0, &queue_error);
				error_a |= queue_error != CL_SUCCESS;
				if (!error_a)
				{
					cl_int attempt_error (0);
					attempt_buffer = clCreateBuffer (context, CL_MEM_READ_ONLY | CL_MEM_HOST_WRITE_ONLY, sizeof (uint64_t), nullptr, &attempt_error);
					error_a |= attempt_error != CL_SUCCESS;
					if (!error_a)
					{
						cl_int result_error (0);
						result_buffer = clCreateBuffer (context, CL_MEM_WRITE_ONLY | CL_MEM_HOST_READ_ONLY, sizeof (uint64_t), nullptr, &result_error);
						error_a |= result_error != CL_SUCCESS;
						if (!error_a)
						{
							cl_int item_error (0);
							size_t item_size (sizeof (nano::uint256_union));
							item_buffer = clCreateBuffer (context, CL_MEM_READ_ONLY | CL_MEM_HOST_WRITE_ONLY, item_size, nullptr, &item_error);
							error_a |= item_error != CL_SUCCESS;
							if (!error_a)
							{
								cl_int difficulty_error (0);
								difficulty_buffer = clCreateBuffer (context, CL_MEM_READ_ONLY | CL_MEM_HOST_WRITE_ONLY, sizeof (uint64_t), nullptr, &difficulty_error);
								error_a |= difficulty_error != CL_SUCCESS;
								if (!error_a)
								{
									cl_int program_error (0);
									char const * program_data (opencl_program.data ());
									size_t program_length (opencl_program.size ());
									program = clCreateProgramWithSource (context, 1, &program_data, &program_length, &program_error);
									error_a |= program_error != CL_SUCCESS;
									if (!error_a)
									{
										auto clBuildProgramError (clBuildProgram (program, static_cast<cl_uint> (selected_devices.size ()), selected_devices.data (), "-D __APPLE__", nullptr, nullptr));
										error_a |= clBuildProgramError != CL_SUCCESS;
										if (!error_a)
										{
											cl_int kernel_error (0);
											kernel = clCreateKernel (program, "nano_work", &kernel_error);
											error_a |= kernel_error != CL_SUCCESS;
											if (!error_a)
											{
												cl_int arg0_error (clSetKernelArg (kernel, 0, sizeof (attempt_buffer), &attempt_buffer));
												error_a |= arg0_error != CL_SUCCESS;
												if (!error_a)
												{
													cl_int arg1_error (clSetKernelArg (kernel, 1, sizeof (result_buffer), &result_buffer));
													error_a |= arg1_error != CL_SUCCESS;
													if (!error_a)
													{
														cl_int arg2_error (clSetKernelArg (kernel, 2, sizeof (item_buffer), &item_buffer));
														error_a |= arg2_error != CL_SUCCESS;
														if (!error_a)
														{
															cl_int arg3_error (clSetKernelArg (kernel, 3, sizeof (difficulty_buffer), &difficulty_buffer));
															error_a |= arg3_error != CL_SUCCESS;
															if (!error_a)
															{
															}
															else
															{
																logger.always_log (boost::str (boost::format ("Bind argument 3 error %1%") % arg3_error));
															}
														}
														else
														{
															logger.always_log (boost::str (boost::format ("Bind argument 2 error %1%") % arg2_error));
														}
													}
													else
													{
														logger.always_log (boost::str (boost::format ("Bind argument 1 error %1%") % arg1_error));
													}
												}
												else
												{
													logger.always_log (boost::str (boost::format ("Bind argument 0 error %1%") % arg0_error));
												}
											}
											else
											{
												logger.always_log (boost::str (boost::format ("Create kernel error %1%") % kernel_error));
											}
										}
										else
										{
											logger.always_log (boost::str (boost::format ("Build program error %1%") % clBuildProgramError));
											for (auto i (selected_devices.begin ()), n (selected_devices.end ()); i != n; ++i)
											{
												size_t log_size (0);
												clGetProgramBuildInfo (program, *i, CL_PROGRAM_BUILD_LOG, 0, nullptr, &log_size);
												std::vector<char> log (log_size);
												clGetProgramBuildInfo (program, *i, CL_PROGRAM_BUILD_LOG, log.size (), log.data (), nullptr);
												logger.always_log (log.data ());
											}
										}
									}
									else
									{
										logger.always_log (boost::str (boost::format ("Create program error %1%") % program_error));
									}
								}
								else
								{
									logger.always_log (boost::str (boost::format ("Difficulty buffer error %1%") % difficulty_error));
								}
							}
							else
							{
								logger.always_log (boost::str (boost::format ("Item buffer error %1%") % item_error));
							}
						}
						else
						{
							logger.always_log (boost::str (boost::format ("Result buffer error %1%") % result_error));
						}
					}
					else
					{
						logger.always_log (boost::str (boost::format ("Attempt buffer error %1%") % attempt_error));
					}
				}
				else
				{
					logger.always_log (boost::str (boost::format ("Unable to create command queue %1%") % queue_error));
				}
			}
			else
			{
				logger.always_log (boost::str (boost::format ("Unable to create context %1%") % createContextError));
			}
		}
		else
		{
			logger.always_log (boost::str (boost::format ("Requested device %1%, and only have %2%") % config.device % platform.devices.size ()));
		}
	}
	else
	{
		logger.always_log (boost::str (boost::format ("Requested platform %1% and only have %2%") % config.platform % environment_a.platforms.size ()));
	}
}

nano::opencl_work::~opencl_work ()
{
	if (kernel != 0)
	{
		clReleaseKernel (kernel);
	}
	if (program != 0)
	{
		clReleaseProgram (program);
	}
	if (context != 0)
	{
		clReleaseContext (context);
	}
}

boost::optional<uint64_t> nano::opencl_work::generate_work (nano::work_version const version_a, nano::root const & root_a, uint64_t const difficulty_a)
{
	std::atomic<int> ticket_l{ 0 };
	return generate_work (version_a, root_a, difficulty_a, ticket_l);
}

boost::optional<uint64_t> nano::opencl_work::generate_work (nano::work_version const version_a, nano::root const & root_a, uint64_t const difficulty_a, std::atomic<int> & ticket_a)
{
	nano::lock_guard<nano::mutex> lock (mutex);
	bool error (false);
	int ticket_l (ticket_a);
	uint64_t result (0);
	unsigned thread_count (config.threads);
	size_t work_size[] = { thread_count, 0, 0 };
	while (nano::work_difficulty (version_a, root_a, result) < difficulty_a && !error && ticket_a == ticket_l)
	{
		result = rand.next ();
		cl_int write_error1 = clEnqueueWriteBuffer (queue, attempt_buffer, false, 0, sizeof (uint64_t), &result, 0, nullptr, nullptr);
		if (write_error1 == CL_SUCCESS)
		{
			cl_int write_error2 = clEnqueueWriteBuffer (queue, item_buffer, false, 0, sizeof (nano::root), root_a.bytes.data (), 0, nullptr, nullptr);
			if (write_error2 == CL_SUCCESS)
			{
				cl_int write_error3 = clEnqueueWriteBuffer (queue, difficulty_buffer, false, 0, sizeof (uint64_t), &difficulty_a, 0, nullptr, nullptr);
				if (write_error3 == CL_SUCCESS)
				{
					cl_int enqueue_error = clEnqueueNDRangeKernel (queue, kernel, 1, nullptr, work_size, nullptr, 0, nullptr, nullptr);
					if (enqueue_error == CL_SUCCESS)
					{
						cl_int read_error1 = clEnqueueReadBuffer (queue, result_buffer, false, 0, sizeof (uint64_t), &result, 0, nullptr, nullptr);
						if (read_error1 == CL_SUCCESS)
						{
							cl_int finishError = clFinish (queue);
							if (finishError == CL_SUCCESS)
							{
							}
							else
							{
								error = true;
								logger.always_log (boost::str (boost::format ("Error finishing queue %1%") % finishError));
							}
						}
						else
						{
							error = true;
							logger.always_log (boost::str (boost::format ("Error reading result %1%") % read_error1));
						}
					}
					else
					{
						error = true;
						logger.always_log (boost::str (boost::format ("Error enqueueing kernel %1%") % enqueue_error));
					}
				}
				else
				{
					error = true;
					logger.always_log (boost::str (boost::format ("Error writing item %1%") % write_error3));
				}
			}
			else
			{
				error = true;
				logger.always_log (boost::str (boost::format ("Error writing item %1%") % write_error2));
			}
		}
		else
		{
			error = true;
			logger.always_log (boost::str (boost::format ("Error writing attempt %1%") % write_error1));
		}
	}
	boost::optional<uint64_t> value;
	if (!error)
	{
		value = result;
	}
	return value;
}

std::unique_ptr<nano::opencl_work> nano::opencl_work::create (bool create_a, nano::opencl_config const & config_a, nano::logger_mt & logger_a)
{
	std::unique_ptr<nano::opencl_work> result;
	if (create_a)
	{
		auto error (false);
		nano::opencl_environment environment (error);
		std::stringstream stream;
		environment.dump (stream);
		logger_a.always_log (stream.str ());
		if (!error)
		{
			result.reset (new nano::opencl_work (error, config_a, environment, logger_a));
			if (error)
			{
				result.reset ();
			}
		}
	}
	return result;
}
