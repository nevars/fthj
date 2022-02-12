package ru.pgw.ftj.util;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.GridJobContextImpl;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FaultTolerantUtilsTest {

    ComputeJobContext jobContext;

    @Test
    void givenJobPartitions_whenGetPartitions_thenSuccess() {
        jobContext = new GridJobContextImpl(mock(GridKernalContext.class), IgniteUuid.randomUuid());
        jobContext.setAttribute("test", new int[] {1, 2, 3});

        int[] array = jobContext.getAttribute("test");
        assertThat(array).isNotNull().containsExactly(1, 2, 3);
    }
}