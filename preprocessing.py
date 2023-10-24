import numpy as np

def ensure_zero_mean(sig):
    return sig - np.mean(sig)

def band_limit(sig, passbands, fs):
    cur_fft_freqs = np.fft.fftfreq(n = len(sig), d = 1 / fs)
    cur_fft = np.fft.fft(sig)

    masks = []
    for (passband_low, passband_high) in passbands:
        masks.append(np.logical_and(abs(cur_fft_freqs) > passband_low, abs(cur_fft_freqs) < passband_high))

    mask = np.logical_or.reduce(masks)
    cur_fft_filtered = cur_fft * mask

    filtered_sig = np.real_if_close(np.fft.ifft(cur_fft_filtered))
    return filtered_sig
