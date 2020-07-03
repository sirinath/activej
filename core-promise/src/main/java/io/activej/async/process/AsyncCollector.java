/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.async.process;

import io.activej.common.exception.UncheckedException;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiConsumer;

import static io.activej.common.Preconditions.checkState;

@SuppressWarnings("UnusedReturnValue")
public final class AsyncCollector<A> implements AsyncCloseable {
	private final SettablePromise<A> resultPromise = new SettablePromise<>();
	private boolean started;

	@Nullable
	private A accumulator;

	private int activePromises;

	private AsyncCollector(@Nullable A accumulator) {
		this.accumulator = accumulator;
	}

	public static <A> AsyncCollector<A> create(@Nullable A accumulator) {
		return new AsyncCollector<>(accumulator);
	}

	public <T> AsyncCollector<A> withPromise(@NotNull Promise<T> promise, @NotNull BiConsumer<A, T> accumulator) {
		addPromise(promise, accumulator);
		return this;
	}

	public Promise<A> run() {
		checkState(!started);
		this.started = true;
		if (activePromises == 0 && !resultPromise.isComplete()) {
			resultPromise.set(accumulator);
			accumulator = null;
		}
		return get();
	}

	public Promise<A> run(@NotNull Promise<Void> runtimePromise) {
		addPromise(runtimePromise, (result, v) -> {});
		return run();
	}

	@SuppressWarnings("unchecked")
	public <T> Promise<T> addPromise(@NotNull Promise<T> promise, @NotNull BiConsumer<A, T> consumer) {
		if (resultPromise.isException()) return (Promise<T>) resultPromise;
		activePromises++;
		return promise.whenComplete((v, e) -> {
			activePromises--;
			if (resultPromise.isComplete()) return;
			if (e == null) {
				try {
					consumer.accept(accumulator, v);
				} catch (UncheckedException u) {
					resultPromise.setException(u.getCause());
					accumulator = null;
					return;
				}
				if (activePromises == 0 && started) {
					resultPromise.set(accumulator);
					accumulator = null;
				}
			} else {
				resultPromise.setException(e);
				accumulator = null;
			}
		});
	}

	public <V> SettablePromise<V> newPromise(@NotNull BiConsumer<A, V> consumer) {
		SettablePromise<V> resultPromise = new SettablePromise<>();
		addPromise(resultPromise, consumer);
		return resultPromise;
	}

	@NotNull
	public Promise<A> get() {
		return resultPromise;
	}

	@Nullable
	public A getAccumulator() {
		return accumulator;
	}

	public int getActivePromises() {
		return activePromises;
	}

	public void complete() {
		resultPromise.trySet(accumulator);
		accumulator = null;
	}

	public void complete(A result) {
		resultPromise.trySet(result);
		accumulator = null;
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		resultPromise.trySetException(e);
		accumulator = null;
	}
}
